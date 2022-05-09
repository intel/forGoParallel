package pipeline

import (
	"sync"
)

type parnode[T any] struct {
	waitGroup  sync.WaitGroup
	filters    []Filter[T]
	receivers  []Receiver[T]
	finalizers []Finalizer
}

// Par creates a parallel node with the given filters.
func Par[T any](filters ...Filter[T]) Node[T] {
	return &parnode[T]{filters: filters}
}

// Implements the TryMerge method of the Node interface.
func (node *parnode[T]) TryMerge(next Node[T]) bool {
	if nxt, merge := next.(*parnode[T]); merge {
		node.filters = append(node.filters, nxt.filters...)
		node.receivers = append(node.receivers, nxt.receivers...)
		node.finalizers = append(node.finalizers, nxt.finalizers...)
		return true
	}
	return false
}

// Implements the Begin method of the Node interface.
func (node *parnode[T]) Begin(p *Pipeline[T], _ int, dataSize *int) (keep bool) {
	node.receivers, node.finalizers = ComposeFilters(p, Parallel, dataSize, node.filters)
	node.filters = nil
	keep = (len(node.receivers) > 0) || (len(node.finalizers) > 0)
	return
}

func (node *parnode[T]) StrictOrd() bool {
	return false
}

// Implements the Feed method of the Node interface.
func (node *parnode[T]) Feed(p *Pipeline[T], index int, seqNo int, data T) {
	node.waitGroup.Add(1)
	go func() {
		defer node.waitGroup.Done()
		select {
		case <-p.ctx.Done():
			return
		default:
			feed(p, node.receivers, index, seqNo, data)
		}
	}()
}

// Implements the End method of the Node interface.
func (node *parnode[T]) End() {
	node.waitGroup.Wait()
	for _, finalize := range node.finalizers {
		finalize()
	}
	node.receivers = nil
	node.finalizers = nil
}
