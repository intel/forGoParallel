package gsync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// AtomicPointer enables type-safe atomic operations on pointer values.
type AtomicPointer[T any] struct{ ptr unsafe.Pointer }

func MakeAtomicPointer[T any](value *T) AtomicPointer[T] {
	return AtomicPointer[T]{unsafe.Pointer(value)}
}

func (ptr *AtomicPointer[T]) CompareAndSwap(old, new *T) (swapped bool) {
	return atomic.CompareAndSwapPointer(&ptr.ptr, unsafe.Pointer(old), unsafe.Pointer(new))
}

func (ptr *AtomicPointer[T]) Load() *T {
	return (*T)(atomic.LoadPointer(&ptr.ptr))
}

func (ptr *AtomicPointer[T]) Store(value *T) {
	atomic.StorePointer(&ptr.ptr, unsafe.Pointer(value))
}

func (ptr *AtomicPointer[T]) Swap(new *T) (old *T) {
	return (*T)(atomic.SwapPointer(&ptr.ptr, unsafe.Pointer(new)))
}

// Map is a type-safe version of sync.Map.
type Map[K comparable, V any] sync.Map

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

// Pool is a type-safe version of sync.Pool.
type Pool[T any] struct {
	New      func() *T
	syncPool AtomicPointer[sync.Pool]
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
