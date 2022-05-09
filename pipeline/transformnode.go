package pipeline

type transformNode[S, T any] struct {
	next      *Pipeline[T]
	transform func(seqNo int, data S) T
}

func Transform[S, T any](p *Pipeline[S], transform func(int, S) T) *Pipeline[T] {
	next := &Pipeline[T]{
		pipelineState: p.pipelineState,
	}
	p.Add(&transformNode[S, T]{
		next:      next,
		transform: transform,
	})
	return next
}

func OrdTransform[S, T any](p *Pipeline[S], transform func(int, S) T) *Pipeline[T] {
	next := &Pipeline[T]{
		pipelineState: p.pipelineState,
	}
	p.Add(
		Ord(Receive(func(_ int, data S) S {
			return data
		})),
		&transformNode[S, T]{
			next:      next,
			transform: transform,
		})
	return next
}

func SeqTransform[S, T any](p *Pipeline[S], transform func(int, S) T) *Pipeline[T] {
	next := &Pipeline[T]{
		pipelineState: p.pipelineState,
	}
	p.Add(
		Seq(Receive(func(_ int, data S) S {
			return data
		})),
		&transformNode[S, T]{
			next:      next,
			transform: transform,
		})
	return next
}

func ParTransform[S, T any](p *Pipeline[S], transform func(int, S) T) *Pipeline[T] {
	next := &Pipeline[T]{
		pipelineState: p.pipelineState,
	}
	p.Add(
		Par(Receive(func(_ int, data S) S {
			return data
		})),
		&transformNode[S, T]{
			next:      next,
			transform: transform,
		})
	return next
}

func StrictOrdTransform[S, T any](p *Pipeline[S], transform func(int, S) T) *Pipeline[T] {
	next := &Pipeline[T]{
		pipelineState: p.pipelineState,
	}
	p.Add(
		StrictOrd(Receive(func(_ int, data S) S {
			return data
		})),
		&transformNode[S, T]{
			next:      next,
			transform: transform,
		})
	return next
}

func LimitedParTransform[S, T any](p *Pipeline[S], transform func(int, S) T) *Pipeline[T] {
	next := &Pipeline[T]{
		pipelineState: p.pipelineState,
	}
	p.Add(
		LimitedPar(Receive(func(_ int, data S) S {
			return data
		})),
		&transformNode[S, T]{
			next:      next,
			transform: transform,
		})
	return next
}

func (node *transformNode[S, T]) TryMerge(_ Node[S]) (merged bool) {
	return false
}

func (node *transformNode[S, T]) Begin(_ *Pipeline[S], _ int, dataSize *int) (keep bool) {
	for index := 0; index < len(node.next.nodes); {
		if node.next.nodes[index].Begin(node.next, index, dataSize) {
			index++
		} else {
			node.next.nodes = append(node.next.nodes[:index], node.next.nodes[index+1:]...)
		}
	}
	if node.next.err != nil {
		return
	}
	if len(node.next.nodes) == 0 {
		return false
	}
	for index := 0; index < len(node.next.nodes)-1; {
		if node.next.nodes[index].TryMerge(node.next.nodes[index+1]) {
			node.next.nodes = append(node.next.nodes[:index+1], node.next.nodes[index+2:]...)
		} else {
			index++
		}
	}
	return true
}

func (node *transformNode[S, T]) StrictOrd() bool {
	for index := len(node.next.nodes) - 1; index >= 0; index-- {
		if node.next.nodes[index].StrictOrd() {
			for index = index - 1; index >= 0; index-- {
				switch node := node.next.nodes[index].(type) {
				case *seqnode[T]:
					node.kind = Ordered
				case *lparnode[T]:
					node.makeOrdered()
				}
			}
			return true
		}
	}
	return false
}

func (node *transformNode[S, T]) Feed(_ *Pipeline[S], _ int, seqNo int, data S) {
	node.next.FeedForward(-1, seqNo, node.transform(seqNo, data))
}

func (node *transformNode[S, T]) End() {
	for _, node := range node.next.nodes {
		node.End()
	}
}
