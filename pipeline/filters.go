package pipeline

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Identity is a filter that passes data batches through unmodified.
// This filter will be optimized away in a pipeline, so it
// does not hurt to add it.
func Identity[T any](_ *Pipeline[T], _ NodeKind, _ *int) (_ Receiver[T], _ Finalizer) {
	return
}

// Receive creates a Filter that returns the given receiver and a nil finalizer.
func Receive[T any](receive Receiver[T]) Filter[T] {
	return func(_ *Pipeline[T], _ NodeKind, _ *int) (receiver Receiver[T], _ Finalizer) {
		receiver = receive
		return
	}
}

// Finalize creates a filter that returns a nil receiver and the given
// finalizer.
func Finalize[T any](finalize Finalizer) Filter[T] {
	return func(_ *Pipeline[T], _ NodeKind, _ *int) (_ Receiver[T], finalizer Finalizer) {
		finalizer = finalize
		return
	}
}

// ReceiveAndFinalize creates a filter that returns the given filter and
// receiver.
func ReceiveAndFinalize[T any](receive Receiver[T], finalize Finalizer) Filter[T] {
	return func(_ *Pipeline[T], _ NodeKind, _ *int) (receiver Receiver[T], finalizer Finalizer) {
		receiver = receive
		finalizer = finalize
		return
	}
}

// A Predicate is a function that is passed a data batch and returns a boolean
// value.
//
// In most cases, it will cast the data parameter to a specific slice type and
// check a predicate on each element of the slice.
type Predicate[T any] func(data T) bool

// Every creates a filter that sets the result pointer to true if the given
// predicate returns true for every data batch. If cancelWhenKnown is true, this
// filter cancels the pipeline as soon as the predicate returns false on a data
// batch.
func Every[T any](result *bool, cancelWhenKnown bool, predicate Predicate[T]) Filter[T] {
	*result = true
	return func(pipeline *Pipeline[T], kind NodeKind, _ *int) (receiver Receiver[T], finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(1)
			receiver = func(_ int, data T) T {
				if !predicate(data) {
					atomic.StoreInt32(&res, 0)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if atomic.LoadInt32(&res) == 0 {
					*result = false
				}
			}
		default:
			receiver = func(_ int, data T) T {
				if !predicate(data) {
					*result = false
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

// NotEvery creates a filter that sets the result pointer to true if the given
// predicate returns false for at least one of the data batches it is passed. If
// cancelWhenKnown is true, this filter cancels the pipeline as soon as the
// predicate returns false on a data batch.
func NotEvery[T any](result *bool, cancelWhenKnown bool, predicate Predicate[T]) Filter[T] {
	*result = false
	return func(pipeline *Pipeline[T], kind NodeKind, _ *int) (receiver Receiver[T], finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(0)
			receiver = func(_ int, data T) T {
				if !predicate(data) {
					atomic.StoreInt32(&res, 1)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if atomic.LoadInt32(&res) == 1 {
					*result = true
				}
			}
		default:
			receiver = func(_ int, data T) T {
				if !predicate(data) {
					*result = true
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

// Some creates a filter that sets the result pointer to true if the given
// predicate returns true for at least one of the data batches it is passed. If
// cancelWhenKnown is true, this filter cancels the pipeline as soon as the
// predicate returns true on a data batch.
func Some[T any](result *bool, cancelWhenKnown bool, predicate Predicate[T]) Filter[T] {
	*result = false
	return func(pipeline *Pipeline[T], kind NodeKind, _ *int) (receiver Receiver[T], finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(0)
			receiver = func(_ int, data T) T {
				if predicate(data) {
					atomic.StoreInt32(&res, 1)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if atomic.LoadInt32(&res) == 1 {
					*result = true
				}
			}
		default:
			receiver = func(_ int, data T) T {
				if predicate(data) {
					*result = true
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

// NotAny creates a filter that sets the result pointer to true if the given
// predicate returns false for every data batch. If cancelWhenKnown is true,
// this filter cancels the pipeline as soon as the predicate returns true on a
// data batch.
func NotAny[T any](result *bool, cancelWhenKnown bool, predicate Predicate[T]) Filter[T] {
	*result = true
	return func(pipeline *Pipeline[T], kind NodeKind, _ *int) (receiver Receiver[T], finalizer Finalizer) {
		switch kind {
		case Parallel:
			res := int32(1)
			receiver = func(_ int, data T) T {
				if predicate(data) {
					atomic.StoreInt32(&res, 0)
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
			finalizer = func() {
				if atomic.LoadInt32(&res) == 0 {
					*result = false
				}
			}
		default:
			receiver = func(_ int, data T) T {
				if predicate(data) {
					*result = false
					if cancelWhenKnown {
						pipeline.Cancel()
					}
				}
				return data
			}
		}
		return
	}
}

// Append creates a filter that appends all the data batches it sees to the
// result. The result must represent a settable slice, for example by using the
// address operator & on a given slice.
func Append[T any](result *[]T) Filter[[]T] {
	return func(pipeline *Pipeline[[]T], kind NodeKind, _ *int) (receiver Receiver[[]T], finalizer Finalizer) {
		switch kind {
		case Parallel:
			var m sync.Mutex
			receiver = func(_ int, data []T) []T {
				if data != nil {
					m.Lock()
					defer m.Unlock()
					*result = append(*result, data...)
				}
				return data
			}
		default:
			receiver = func(_ int, data []T) []T {
				if data != nil {
					*result = append(*result, data...)
				}
				return data
			}
		}
		return
	}
}

// Count creates a filter that sets the result pointer to the total size of all
// data batches it sees.
func Count[T any](result *int) Filter[[]T] {
	return func(pipeline *Pipeline[[]T], kind NodeKind, size *int) (receiver Receiver[[]T], finalizer Finalizer) {
		switch {
		case *size >= 0:
			*result = *size
		case kind == Parallel:
			var res = int64(0)
			receiver = func(_ int, data []T) []T {
				atomic.AddInt64(&res, int64(len(data)))
				return data
			}
			finalizer = func() {
				*result = int(atomic.LoadInt64(&res))
			}
		default:
			receiver = func(_ int, data []T) []T {
				*result += len(data)
				return data
			}
		}
		return
	}
}

// Limit creates an ordered node with a filter that caps the total size of all
// data batches it passes to the next filter in the pipeline to the given limit.
// If cancelWhenKnown is true, this filter cancels the pipeline as soon as the
// limit is reached. If limit is negative, all data is passed through
// unmodified.
func Limit[T any](limit int, cancelWhenReached bool) Node[[]T] {
	return Ord(func(pipeline *Pipeline[[]T], _ NodeKind, size *int) (receiver Receiver[[]T], _ Finalizer) {
		switch {
		case limit < 0: // unlimited
		case limit == 0:
			*size = 0
			if cancelWhenReached {
				pipeline.Cancel()
			}
			receiver = func(_ int, _ []T) []T { return nil }
		case (*size < 0) || (*size > limit):
			if *size > 0 {
				*size = limit
			}
			seen := 0
			receiver = func(_ int, data []T) (result []T) {
				if seen >= limit {
					return
				}
				l := len(data)
				if (seen + l) > limit {
					result = data[:limit-seen]
					seen = limit
				} else {
					result = data
					seen += l
				}
				if cancelWhenReached && (seen == limit) {
					pipeline.Cancel()
				}
				return
			}
		}
		return
	})
}

// Skip creates an ordered node with a filter that skips the first n elements
// from the data batches it passes to the next filter in the pipeline. If n is
// negative, no data is passed through, and the error value of the pipeline is
// set to a non-nil value.
func Skip[T any](n int) Node[[]T] {
	return Ord(func(pipeline *Pipeline[[]T], _ NodeKind, size *int) (receiver Receiver[[]T], _ Finalizer) {
		switch {
		case n < 0: // skip everything
			*size = 0
			pipeline.SetErr(errors.New("skip filter with unknown size"))
			receiver = func(_ int, _ []T) []T { return nil }
		case n == 0: // nothing to skip
		case (*size < 0) || (*size > n):
			if *size > 0 {
				*size = n
			}
			seen := 0
			receiver = func(_ int, data []T) (result []T) {
				if seen >= n {
					result = data
					return
				}
				l := len(data)
				if (seen + l) > n {
					result = data[n-seen:]
					seen = n
				} else {
					seen += l
				}
				return
			}
		case *size <= n:
			*size = 0
			receiver = func(_ int, _ []T) []T { return nil }
		}
		return
	})
}
