package pipeline

import (
	"sync"
)

type (
	dataBatch[T any] struct {
		seqNo int
		data  T
	}

	seqnode[T any] struct {
		kind       NodeKind
		channel    chan dataBatch[T]
		waitGroup  sync.WaitGroup
		filters    []Filter[T]
		receivers  []Receiver[T]
		finalizers []Finalizer
	}
)

// Ord creates an ordered node with the given filters.
func Ord[T any](filters ...Filter[T]) Node[T] {
	return &seqnode[T]{kind: Ordered, filters: filters}
}

// Seq creates a sequential node with the given filters.
func Seq[T any](filters ...Filter[T]) Node[T] {
	return &seqnode[T]{kind: Sequential, filters: filters}
}

// Implements the TryMerge method of the Node interface.
func (node *seqnode[T]) TryMerge(next Node[T]) bool {
	if nxt, merge := next.(*seqnode[T]); merge && (len(nxt.filters) > 0) {
		if nxt.kind == Ordered {
			node.kind = Ordered
		}
		node.filters = append(node.filters, nxt.filters...)
		node.receivers = append(node.receivers, nxt.receivers...)
		node.finalizers = append(node.finalizers, nxt.finalizers...)
		return true
	}
	return false
}

// Implements the Begin method of the Node interface.
func (node *seqnode[T]) Begin(p *Pipeline[T], index int, dataSize *int) (keep bool) {
	node.receivers, node.finalizers = ComposeFilters(p, node.kind, dataSize, node.filters)
	node.filters = nil
	if keep = (len(node.receivers) > 0) || (len(node.finalizers) > 0); keep {
		node.channel = make(chan dataBatch[T])
		node.waitGroup.Add(1)
		switch node.kind {
		case Sequential:
			go func() {
				defer node.waitGroup.Done()
				for {
					select {
					case <-p.ctx.Done():
						return
					case batch, ok := <-node.channel:
						if !ok {
							return
						}
						feed(p, node.receivers, index, batch.seqNo, batch.data)
					}
				}
			}()
		case Ordered:
			go func() {
				defer node.waitGroup.Done()
				stash := make(map[int]T)
				run := 0
				for {
					select {
					case <-p.ctx.Done():
						return
					case batch, ok := <-node.channel:
						switch {
						case !ok:
							return
						case batch.seqNo < run:
							panic("Invalid receive order in an ordered pipeline node.")
						case batch.seqNo > run:
							stash[batch.seqNo] = batch.data
						default:
							feed(p, node.receivers, index, batch.seqNo, batch.data)
						checkStash:
							for {
								select {
								case <-p.ctx.Done():
									return
								default:
									run++
									data, ok := stash[run]
									if !ok {
										break checkStash
									}
									delete(stash, run)
									feed(p, node.receivers, index, run, data)
								}
							}
						}
					}
				}
			}()
		default:
			panic("Invalid NodeKind in a sequential pipeline node.")
		}
	}
	return
}

// Implements the Feed method of the Node interface.
func (node *seqnode[T]) Feed(p *Pipeline[T], _ int, seqNo int, data T) {
	select {
	case <-p.ctx.Done():
		return
	case node.channel <- dataBatch[T]{seqNo, data}:
		return
	}
}

// Implements the End method of the Node interface.
func (node *seqnode[T]) End() {
	close(node.channel)
	node.waitGroup.Wait()
	for _, finalize := range node.finalizers {
		finalize()
	}
	node.receivers = nil
	node.finalizers = nil
}
