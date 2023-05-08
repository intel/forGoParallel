// Package parallel provides functions for expressing parallel algorithms.
package parallel

import (
	"fmt"
	"math"
	"sync"

	"github.com/intel/forGoParallel/internal"
)

// Reduce receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine, and Reduce returns only when
// all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and Reduce eventually panics with the left-most recovered panic
// value.
func Reduce[T any](
	join func(x, y T) T,
	firstFunction func() T,
	moreFunctions ...func() T,
) T {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right T
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = moreFunctions[0]()
		}()
		left = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = Reduce(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left = Reduce(join, firstFunction, moreFunctions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return join(left, right)
}

type Addable interface {
	~uint | ~int | ~uintptr |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~int8 | ~int16 | ~int32 | ~int64 |
		~float32 | ~float64 |
		~complex64 | ~complex128 |
		~string
}

// ReduceSum receives zero or more functions, executes them in parallel,
// and adds their results in parallel.
//
// Each function is invoked in its own goroutine, and ReduceSum returns
// only when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceSum eventually panics with the left-most recovered
// panic value.
func ReduceSum[T Addable](functions ...func() T) (result T) {
	switch len(functions) {
	case 0:
		return
	case 1:
		return functions[0]()
	}
	var left, right T
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(functions) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = functions[1]()
		}()
		left = functions[0]()
	default:
		half := len(functions) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceSum(functions[half:]...)
		}()
		left = ReduceSum(functions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return left + right
}

type Multipliable interface {
	~uint | ~int | ~uintptr |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~int8 | ~int16 | ~int32 | ~int64 |
		~float32 | ~float64 |
		~complex64 | ~complex128
}

// ReduceProduct receives zero or more functions, executes them in
// parallel, and multiplies their results in parallel.
//
// Each function is invoked in its own goroutine, and ReduceProduct
// returns only when all functions have terminated.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and ReduceProduct eventually panics with the left-most
// recovered panic value.
func ReduceProduct[T Multipliable](functions ...func() T) T {
	switch len(functions) {
	case 0:
		return 1
	case 1:
		return functions[0]()
	}
	var left, right T
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(functions) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = functions[1]()
		}()
		left = functions[0]()
	default:
		half := len(functions) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right = ReduceProduct(functions[half:]...)
		}()
		left = ReduceProduct(functions[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return left * right
}

// Do receives zero or more thunks and executes them in parallel.
//
// Each thunk is invoked in its own goroutine, and Do returns only when all
// thunks have terminated.
//
// If one or more thunks panic, the corresponding goroutines recover the panics,
// and Do eventually panics with the left-most recovered panic value.
func Do(thunks ...func()) {
	switch len(thunks) {
	case 0:
		return
	case 1:
		thunks[0]()
		return
	}
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(thunks) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			thunks[1]()
		}()
		thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			Do(thunks[half:]...)
		}()
		Do(thunks[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
}

// And receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and And returns only when all
// predicates have terminated, combining all return values with the && operator,
// with true as the default return value.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and And eventually panics with the left-most recovered panic value.
func And(predicates ...func() bool) bool {
	switch len(predicates) {
	case 0:
		return true
	case 1:
		return predicates[0]()
	}
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(predicates) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = And(predicates[half:]...)
		}()
		b0 = And(predicates[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b0 && b1
}

// Or receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and Or returns only when all
// predicates have terminated, combining all return values with the || operator,
// with false as the default return value.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and Or eventually panics with the left-most recovered panic value.
func Or(predicates ...func() bool) bool {
	switch len(predicates) {
	case 0:
		return false
	case 1:
		return predicates[0]()
	}
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	switch len(predicates) {
	case 2:
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = predicates[1]()
		}()
		b0 = predicates[0]()
	default:
		half := len(predicates) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = Or(predicates[half:]...)
		}()
		b0 = Or(predicates[:half]...)
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b0 || b1
}

// Range receives a range, a batch count n, and a range function f, divides the
// range into batches, and invokes the range function for each of these batches
// in parallel, covering the half-open interval from low to high, including low
// but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtimes.NumCPU()
// into account.
//
// The range function is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and Range returns only when all range functions have terminated.
//
// Range panics if high < low, or if n < 0.
//
// If one or more range function invocations panic, the corresponding goroutines
// recover the panics, and Range eventually panics with the left-most recovered
// panic value.
func Range(low, high, n int, f func(low, high int)) {
	var recur func(int, int, int)
	recur = func(low, high, n int) {
		switch {
		case n == 1:
			f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				f(low, high)
				return
			}
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				recur(mid, high, n-half)
			}()
			recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeAnd receives a range, a batch count n, and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches in parallel, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.NumCPU()
// into account.
//
// The range predicate is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeAnd returns only when all range predicates have
// terminated, combining all return values with the && operator.
//
// RangeAnd panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and RangeAnd eventually panics with the
// left-most recovered panic value.
func RangeAnd(low, high, n int, f func(low, high int) bool) bool {
	var recur func(int, int, int) bool
	recur = func(low, high, n int) bool {
		switch {
		case n == 1:
			return f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return f(low, high)
			}
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				b1 = recur(mid, high, n-half)
			}()
			b0 = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b0 && b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeOr receives a range, a batch count n, and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches in parallel, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.NumCPU()
// into account.
//
// The range predicate is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeOr returns only when all range predicates have
// terminated, combining all return values with the || operator.
//
// RangeOr panics if high < low, or if n < 0.
//
// If one or more range predicate invocations panic, the corresponding
// goroutines recover the panics, and RangeOr eventually panics with the
// left-most recovered panic value.
func RangeOr(low, high, n int, f func(low, high int) bool) bool {
	var recur func(int, int, int) bool
	recur = func(low, high, n int) bool {
		switch {
		case n == 1:
			return f(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return f(low, high)
			}
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				b1 = recur(mid, high, n-half)
			}()
			b0 = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b0 || b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduce receives a range, a batch count, a range reduce function, and a
// join function, divides the range into batches, and invokes the range reducer
// for each of these batches in parallel, covering the half-open interval from
// low to high, including low but excluding high. The results of the range
// reducer invocations are then combined by repeated invocations of join.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.NumCPU()
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduce returns only when all range reducers and pair
// reducers have terminated.
//
// RangeReduce panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduce eventually panics with the left-most
// recovered panic value.
func RangeReduce[T any](
	low, high, n int,
	reduce func(low, high int) T,
	join func(x, y T) T,
) T {
	var recur func(int, int, int) T
	recur = func(low, high, n int) T {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			var left, right T
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceSum receives a range, a batch count, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches in parallel, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then added together.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.NumCPU()
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceIntSum returns only when all range reducers and
// pair reducers have terminated.
//
// RangeReduceIntSum panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceIntSum eventually panics with the
// left-most recovered panic value.
func RangeReduceSum[T Addable](low, high, n int, reduce func(low, high int) T) T {
	var recur func(int, int, int) T
	recur = func(low, high, n int) T {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			var left, right T
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return left + right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

// RangeReduceProduct receives a range, a batch count, and a range reducer
// function, divides the range into batches, and invokes the range reducer for
// each of these batches in parallel, covering the half-open interval from low
// to high, including low but excluding high. The results of the range reducer
// invocations are then multiplied with each other.
//
// The range is specified by a low and high integer, with low <= high. The
// batches are determined by dividing up the size of the range (high - low) by
// n. If n is 0, a reasonable default is used that takes runtime.NumCPU()
// into account.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduceIntProduct returns only when all range reducers
// and pair reducers have terminated.
//
// RangeReduceIntProduct panics if high < low, or if n < 0.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduceIntProducet eventually panics with the
// left-most recovered panic value.
func RangeReduceProduct[T Multipliable](
	low, high, n int,
	reduce func(low, high int) T,
) T {
	var recur func(int, int, int) T
	recur = func(low, high, n int) T {
		switch {
		case n == 1:
			return reduce(low, high)
		case n > 1:
			batchSize := ((high - low - 1) / n) + 1
			half := n / 2
			mid := low + batchSize*half
			if mid >= high {
				return reduce(low, high)
			}
			var left, right T
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right = recur(mid, high, n-half)
			}()
			left = recur(low, mid, half)
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return left * right
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high, n))
}

func PrefixSum[T Addable](slice []T) []T {
	result := make([]T, len(slice))
	for i, k := 0, int(math.Ceil(math.Log2(float64(len(slice))))); i < k; i++ {
		l := int(math.Exp2(float64(i)))
		Do(func() {
			Range(0, l, 0, func(low, high int) {
				for j := low; j < high; j++ {
					result[j] = slice[j]
				}
			})
		}, func() {
			Range(l, len(slice), 0, func(low, high int) {
				for j := low; j < high; j++ {
					result[j] = slice[j-l] + slice[j]
				}
			})
		})
		slice, result = result, slice
	}
	return slice
}
