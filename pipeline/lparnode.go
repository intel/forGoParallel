package pipeline

import (
	"runtime"
	"sync"
)

type lparnode[T any] struct {
	limit      int
	ordered    bool
	cond       *sync.Cond
	channel    chan dataBatch[T]
	waitGroup  sync.WaitGroup
	run        int
	filters    []Filter[T]
	receivers  []Receiver[T]
	finalizers []Finalizer
}

// LimitedPar creates a parallel node with the given filters.
func LimitedPar[T any](filters ...Filter[T]) Node[T] {
	return &lparnode[T]{limit: runtime.NumCPU(), filters: filters}
}

func (node *lparnode[T]) makeOrdered() {
	node.ordered = true
	node.cond = sync.NewCond(&sync.Mutex{})
}

// Implements the TryMerge method of the Node interface.
func (node *lparnode[T]) TryMerge(next Node[T]) bool {
	if nxt, merge := next.(*lparnode[T]); merge && (nxt.limit == node.limit) {
		node.filters = append(node.filters, nxt.filters...)
		node.receivers = append(node.receivers, nxt.receivers...)
		node.finalizers = append(node.finalizers, nxt.finalizers...)
		return true
	}
	return false
}

// Implements the Begin method of the Node interface.
func (node *lparnode[T]) Begin(p *Pipeline[T], index int, dataSize *int) (keep bool) {
	node.receivers, node.finalizers = ComposeFilters(p, Parallel, dataSize, node.filters)
	node.filters = nil
	if keep = (len(node.receivers) > 0) || (len(node.finalizers) > 0); keep {
		node.channel = make(chan dataBatch[T])
		node.waitGroup.Add(node.limit)
		for i := 0; i < node.limit; i++ {
			go func() {
				defer node.waitGroup.Done()
				for {
					select {
					case <-p.ctx.Done():
						if node.ordered {
							node.cond.Broadcast()
						}
						return
					case batch, ok := <-node.channel:
						if !ok {
							return
						}
						if node.ordered {
							node.cond.L.Lock()
							if batch.seqNo != node.run {
								panic("Invalid receive order in an ordered limited parallel pipeline node.")
							}
							node.run++
							node.cond.L.Unlock()
							node.cond.Broadcast()
						}
						feed(p, node.receivers, index, batch.seqNo, batch.data)
					}
				}
			}()
		}
	}
	return
}

func (node *lparnode[T]) StrictOrd() bool {
	return false
}

// Implements the Feed method of the Node interface.
func (node *lparnode[T]) Feed(p *Pipeline[T], _ int, seqNo int, data T) {
	if node.ordered {
		node.cond.L.Lock()
		defer node.cond.L.Unlock()
		for {
			if node.run == seqNo {
				select {
				case <-p.ctx.Done():
					return
				case node.channel <- dataBatch[T]{seqNo, data}:
					return
				}
			}
			select {
			case <-p.ctx.Done():
				return
			default:
				node.cond.Wait()
			}
		}
	}
	select {
	case <-p.ctx.Done():
		return
	case node.channel <- dataBatch[T]{seqNo, data}:
		return
	}
}

// Implements the End method of the Node interface.
func (node *lparnode[T]) End() {
	close(node.channel)
	node.waitGroup.Wait()
	for _, finalize := range node.finalizers {
		finalize()
	}
	node.receivers = nil
	node.finalizers = nil
}
