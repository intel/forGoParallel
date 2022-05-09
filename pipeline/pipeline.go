// Package pipeline provides means to construct and execute parallel pipelines.
//
// A Pipeline feeds batches of data through several functions that can be
// specified to be executed in encounter order, in arbitrary sequential order,
// or in parallel.  Ordered, sequential, or parallel stages can arbitrarily
// alternate.
//
// A Pipeline consists of a Source object, and several Node objects.
//
// Source objects that are supported by this implementation are arrays, slices,
// strings, channels, and bufio.Scanner objects, but other kinds of Source
// objects can be added by user programs.
//
// Node objects can be specified to receive batches from the input source either
// sequentially in encounter order, which is always the same order in which they
// were originally encountered at the source; sequentially, but in arbitrary
// order; or in parallel. Ordered nodes always receive batches in encounter
// order even if they are preceded by arbitrary sequential, or even parallel
// nodes.
//
// Node objects consist of filters, which are pairs of receiver and finalizer
// functions. Each batch is passed to each receiver function, which can
// transform and modify the batch for the next receiver function in the
// pipeline. Each finalizer function is called once when all batches have been
// passed through all receiver functions.
//
// Pipelines do not have an explicit representation for sinks. Instead, filters
// can use side effects to generate results.
//
// Pipelines also support cancelation by way of the context package of Go's
// standard library.
//
// Type parameter support for pipelines is preliminary. At the moment, parameters
// passed from one pipeline stage to the next have to be all of the same type.
// The goal is to change this in the future, but this will require a redesign
// of the API in this package. If you need to change parameter types from one
// stage to the next, you currently need to use a Pipeline[any], or some
// other suitable interface as a type parameter.
package pipeline

import (
	"context"
	"runtime"
	"sync"
)

// A Node object represents a sequence of filters which are together executed
// either in encounter order, in arbitrary sequential order, or in parallel.
//
// The methods of this interface are typically not called by user programs, but
// rather implemented by specific node types and called by pipelines. Ordered,
// sequential, and parallel nodes are also implemented in this package, so that
// user programs are typically not concerned with Node methods at all.
type Node[T any] interface {

	// TryMerge tries to merge node with the current node by appending its
	// filters to the filters of the current node, which succeeds if both nodes
	// are either sequential or parallel. The return value merged indicates
	// whether merging succeeded.
	TryMerge(node Node[T]) (merged bool)

	// Begin informs this node that the pipeline is going to start to feed
	// batches of data to this node. The pipeline, the index of this node among
	// all the nodes in the pipeline, and the expected total size of all batches
	// combined are passed as parameters.
	//
	// The dataSize parameter is either positive, in which case it indicates the
	// expected total size of all batches that will eventually be passed to this
	// node's Feed method, or it is negative, in which case the expected size is
	// either unknown or too difficult to determine. The dataSize parameter is a
	// pointer whose contents can be modified by Begin, for example if this node
	// increases or decreases the total size for subsequent nodes, or if this
	// node can change dataSize from an unknown to a known value, or vice versa,
	// must change it from a known to an unknown value.
	//
	// A node may decide that, based on the given information, it will actually
	// not need to see any of the batches that are normally going to be passed
	// to it. In that case, it can return false as a result, and its Feed and
	// End method will not be called anymore.  Otherwise, it should return true
	// by default.
	Begin(p *Pipeline[T], index int, dataSize *int) (keep bool)

	// StrictOrd reports whether this node or any contained nodes are StrictOrd
	// nodes.
	StrictOrd() bool

	// Feed is called for each batch of data. The pipeline, the index of this
	// node among all the nodes in the pipeline (which may be different from the
	// index number seen by Begin), the sequence number of the batch (according
	// to the encounter order), and the actual batch of data are passed as
	// parameters.
	//
	// The data parameter contains the batch of data, which is usually a slice
	// of a particular type. After the data has been processed by all filters of
	// this node, the node must call p.FeedForward with exactly the same index
	// and sequence numbers, but a potentially modified batch of data.
	// FeedForward must be called even when the data batch is or becomes empty,
	// to ensure that all sequence numbers are seen by subsequent nodes.
	Feed(p *Pipeline[T], index int, seqNo int, data T)

	// End is called after all batches have been passed to Feed. This allows the
	// node to release resources and call the finalizers of its filters.
	End()
}

type pipelineState struct {
	mutex          sync.RWMutex
	err            error
	ctx            context.Context
	cancel         context.CancelFunc
	nofBatches     int
	batchInc       int
	maxBatchSize   int
	notifiers      []func()
	runWithContext func(ctx context.Context, cancel context.CancelFunc)
}

// A Pipeline is a parallel pipeline that can feed batches of data fetched from
// a source through several nodes that are ordered, sequential, or parallel.
type Pipeline[T any] struct {
	*pipelineState
	source Source[T]
	nodes  []Node[T]
}

// New creates a new pipeline with the given source.
func New[T any](source Source[T]) *Pipeline[T] {
	result := &Pipeline[T]{
		pipelineState: new(pipelineState),
		source:        source,
	}
	result.runWithContext = result.defaultRunWithContext
	return result
}

// Err returns the current error value for this pipeline, which may be nil if no
// error has occurred so far.
//
// Err and SetErr are safe to be concurrently invoked.
func (p *Pipeline[T]) Err() (err error) {
	p.mutex.RLock()
	err = p.err
	p.mutex.RUnlock()
	return err
}

// SetErr attempts to set a new error value for this pipeline, unless it already
// has a non-nil error value. If the attempt is successful, SetErr also cancels
// the pipeline, and returns true. If the attempt is not successful, SetErr
// returns false.
//
// SetErr and Err are safe to be concurrently invoked, for example from the
// different goroutines executing filters of parallel nodes in this pipeline.
func (p *Pipeline[T]) SetErr(err error) bool {
	p.mutex.Lock()
	if p.err == nil {
		p.err = err
		p.mutex.Unlock()
		p.cancel()
		return true
	}
	p.mutex.Unlock()
	return false
}

// Context returns this pipeline's context.
func (p *Pipeline[T]) Context() context.Context {
	return p.ctx
}

// Cancel calls the cancel function of this pipeline's context.
func (p *Pipeline[T]) Cancel() {
	p.cancel()
}

// Add appends nodes to the end of this pipeline.
func (p *Pipeline[T]) Add(nodes ...Node[T]) {
	for _, node := range nodes {
		if l := len(p.nodes); (l == 0) || !p.nodes[l-1].TryMerge(node) {
			p.nodes = append(p.nodes, node)
		}
	}
}

// NofBatches sets or gets the number of batches that are created from the data
// source for this pipeline, if the expected total size for this pipeline's data
// source is known or can be determined easily.
//
// NofBatches can be called safely by user programs before Run or RunWithContext
// is called.
//
// If user programs do not call NofBatches, or call them with a value < 1, then
// the pipeline will choose a reasonable default value that takes
// runtime.GOMAXPROCS(0) into account.
//
// If the expected total size for this pipeline's data source is unknown, or is
// difficult to determine, use SetVariableBatchSize to influence batch sizes.
func (p *Pipeline[T]) NofBatches(n int) (nofBatches int) {
	if n < 1 {
		nofBatches = p.nofBatches
		if nofBatches < 1 {
			nofBatches = runtime.GOMAXPROCS(0)
			p.nofBatches = nofBatches
		}
	} else {
		nofBatches = n
		p.nofBatches = n
	}
	return
}

const (
	defaultBatchInc     = 512
	defaultMaxBatchSize = 4096
)

// SetVariableBatchSize sets the batch size(s) for the batches that are created
// from the data source for this pipeline, if the expected total size for this
// pipeline's data source is unknown or difficult to determine.
//
// SetVariableBatchSize can be called safely by user programs before Run or
// RunWithContext is called.
//
// If user programs do not call SetVariableBatchSize, or pass a value < 1 to any
// of the two parameters, then the pipeline will choose a reasonable default
// value for that respective parameter.
//
// The pipeline will start with batchInc as a batch size, and increase the batch
// size for every subsequent batch by batchInc to accomodate data sources of
// different total sizes. The batch size will never be larger than maxBatchSize,
// though.
//
// If the expected total size for this pipeline's data source is known, or can
// be determined easily, use NofBatches to influence the batch size.
func (p *Pipeline[T]) SetVariableBatchSize(batchInc, maxBatchSize int) {
	p.batchInc = batchInc
	p.maxBatchSize = maxBatchSize
}

func (p *Pipeline[T]) finalizeVariableBatchSize() {
	if p.batchInc < 1 {
		p.batchInc = defaultBatchInc
	}
	if p.maxBatchSize < 1 {
		p.maxBatchSize = defaultMaxBatchSize
	}
}

func (p *Pipeline[T]) nextBatchSize(batchSize int) (result int) {
	result = batchSize + p.batchInc
	if result > p.maxBatchSize {
		result = p.maxBatchSize
	}
	return
}

// Notify installs a thunk that gets invoked just before this pipeline starts running.
// f will be invoked in its own goroutine.
func (p *Pipeline[T]) Notify(f func()) {
	p.notifiers = append(p.notifiers, f)
}

// RunWithContext initiates pipeline execution.
//
// It expects a context and a cancel function as parameters, for example from
// context.WithCancel(context.Background()). It does not ensure that the cancel
// function is called at least once, so this must be ensured by the function
// calling RunWithContext.
//
// RunWithContext should only be called after a data source has been set using
// the Source method, and one or more Node objects have been added to the
// pipeline using the Add method. NofBatches can be called before RunWithContext
// to deal with load imbalance, but this is not necessary since RunWithContext
// chooses a reasonable default value.
//
// RunWithContext prepares the data source, tells each node that batches are
// going to be sent to them by calling Begin, and then fetches batches from the
// data source and sends them to the nodes. Once the data source is depleted,
// the nodes are informed that the end of the data source has been reached.
func (p *Pipeline[T]) RunWithContext(ctx context.Context, cancel context.CancelFunc) {
	p.runWithContext(ctx, cancel)
}

func (p *Pipeline[T]) defaultRunWithContext(ctx context.Context, cancel context.CancelFunc) {
	if p.err != nil {
		return
	}
	p.ctx, p.cancel = ctx, cancel
	for i, notifier := range p.notifiers {
		go notifier()
		p.notifiers[i] = nil
	}
	dataSize := p.source.Prepare(p.ctx)
	filteredSize := dataSize
	for index := 0; index < len(p.nodes); {
		if p.nodes[index].Begin(p, index, &filteredSize) {
			index++
		} else {
			p.nodes = append(p.nodes[:index], p.nodes[index+1:]...)
		}
	}
	if p.err != nil {
		return
	}
	if len(p.nodes) > 0 {
		for index := 0; index < len(p.nodes)-1; {
			if p.nodes[index].TryMerge(p.nodes[index+1]) {
				p.nodes = append(p.nodes[:index+1], p.nodes[index+2:]...)
			} else {
				index++
			}
		}
		for index := len(p.nodes) - 1; index >= 0; index-- {
			if p.nodes[index].StrictOrd() {
				for index = index - 1; index >= 0; index-- {
					switch node := p.nodes[index].(type) {
					case *seqnode[T]:
						node.kind = Ordered
					case *lparnode[T]:
						node.makeOrdered()
					}
				}
				break
			}
		}
		if dataSize < 0 {
			p.finalizeVariableBatchSize()
			for seqNo, batchSize := 0, p.batchInc; p.source.Fetch(batchSize) > 0; seqNo, batchSize = seqNo+1, p.nextBatchSize(batchSize) {
				p.nodes[0].Feed(p, 0, seqNo, p.source.Data())
				if err := p.source.Err(); err != nil {
					p.SetErr(err)
					return
				} else if p.Err() != nil {
					return
				}
			}
		} else {
			batchSize := ((dataSize - 1) / p.NofBatches(0)) + 1
			if batchSize == 0 {
				batchSize = 1
			}
			for seqNo := 0; p.source.Fetch(batchSize) > 0; seqNo++ {
				p.nodes[0].Feed(p, 0, seqNo, p.source.Data())
				if err := p.source.Err(); err != nil {
					p.SetErr(err)
					return
				} else if p.Err() != nil {
					return
				}
			}
		}
	}
	for _, node := range p.nodes {
		node.End()
	}
	if p.err == nil {
		p.err = p.source.Err()
	}
}

// Run initiates pipeline execution by calling
// RunWithContext(context.WithCancel(context.Background())), and ensures that
// the cancel function is called at least once when the pipeline is done.
//
// Run should only be called after a data source has been set using the Source
// method, and one or more Node objects have been added to the pipeline using
// the Add method. NofBatches can be called before Run to deal with load
// imbalance, but this is not necessary since Run chooses a reasonable default
// value.
//
// Run prepares the data source, tells each node that batches are going to be
// sent to them by calling Begin, and then fetches batches from the data source
// and sends them to the nodes. Once the data source is depleted, the nodes are
// informed that the end of the data source has been reached.
func (p *Pipeline[T]) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.RunWithContext(ctx, cancel)
}

// FeedForward must be called in the Feed method of a node to forward a
// potentially modified data batch to the next node in the current pipeline.
//
// FeedForward is used in Node implementations. User programs typically do not
// call FeedForward.
//
// FeedForward must be called with the pipeline received as a parameter by Feed,
// and must pass the same index and seqNo received by Feed. The data parameter
// can be either a modified or an unmodified data batch. FeedForward must always
// be called, even if the data batch is unmodified, and even if the data batch
// is or becomes empty.
func (p *Pipeline[T]) FeedForward(index int, seqNo int, data T) {
	if index++; index < len(p.nodes) {
		p.nodes[index].Feed(p, index, seqNo, data)
	}
}
