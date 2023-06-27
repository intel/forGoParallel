package gsync

import (
	"sync"
	"sync/atomic"
)

// Map is a type-safe version of sync.Map.
type Map[K comparable, V any] sync.Map

func (m *Map[K, V]) CompareAndDelete(key K, value V) (deleted bool) {
	return (*sync.Map)(m).CompareAndDelete(key, value)
}

func (m *Map[K, V]) CompareAndSwap(key K, old, new V) bool {
	return (*sync.Map)(m).CompareAndSwap(key, old, new)
}

func (m *Map[K, V]) Delete(key K) {
	(*sync.Map)(m).Delete(key)
}

func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	v, ok := (*sync.Map)(m).Load(key)
	if ok {
		return v.(V), true
	}
	return
}

func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded := (*sync.Map)(m).LoadAndDelete(key)
	if loaded {
		return v.(V), true
	}
	return
}

func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, loaded := (*sync.Map)(m).LoadOrStore(key, value)
	return v.(V), loaded
}

func (m *Map[K, V]) Range(f func(K, V) bool) {
	(*sync.Map)(m).Range(func(k any, v any) bool {
		return f(k.(K), v.(V))
	})
}

func (m *Map[K, V]) Store(key K, value V) {
	(*sync.Map)(m).Store(key, value)
}

func (m *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	prev, loaded := (*sync.Map)(m).Swap(key, value)
	if loaded {
		previous = prev.(V)
	}
	return
}

// Pool is a type-safe version of sync.Pool.
type Pool[T any] struct {
	New      func() *T
	syncPool atomic.Pointer[sync.Pool]
}

func (p *Pool[T]) getSyncPool() *sync.Pool {
	if result := p.syncPool.Load(); result != nil {
		return result
	}
	result := &sync.Pool{
		New: func() any {
			return p.New()
		},
	}
	if p.syncPool.CompareAndSwap(nil, result) {
		return result
	}
	return p.syncPool.Load()
}

func (p *Pool[T]) Get() *T {
	return p.getSyncPool().Get().(*T)
}

func (p *Pool[T]) Put(x *T) {
	p.getSyncPool().Put(x)
}
