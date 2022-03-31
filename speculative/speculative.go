// Package speculative provides functions for expressing parallel algorithms,
// similar to the functions in package parallel, except that the implementations
// here terminate early when they can.
package speculative

import (
	"fmt"
	"sync"

	"github.com/intel/forGoParallel/internal"
)

// Reduce receives one or more functions, executes them in parallel, and
// combines their results with the join function in parallel.
//
// Each function is invoked in its own goroutine. Reduce returns either when all
// functions have terminated with a second return value of false; or when one or
// more functions return a second return value of true. In the latter case, the
// first return value of the left-most function that returned true as a second
// return value becomes the final result, without waiting for the other
// functions to terminate.
//
// If one or more functions panic, the corresponding goroutines recover the
// panics, and Reduce eventually panics with the left-most recovered panic
// value.
func Reduce[T any](
	join func(x, y T) (T, bool),
	firstFunction func() (T, bool),
	moreFunctions ...func() (T, bool),
) (T, bool) {
	if len(moreFunctions) == 0 {
		return firstFunction()
	}
	var left, right T
	var b0, b1 bool
	var p interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	if len(moreFunctions) == 1 {
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = moreFunctions[0]()
		}()
		left, b0 = firstFunction()
	} else {
		half := (len(moreFunctions) + 1) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			right, b1 = Reduce(join, moreFunctions[half], moreFunctions[half+1:]...)
		}()
		left, b0 = Reduce(join, firstFunction, moreFunctions[:half]...)
	}
	if b0 {
		return left, true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	if b1 {
		return right, true
	}
	return join(left, right)
}

// Do receives zero or more thunks and executes them in parallel.
//
// Each function is invoked in its own goroutine. Do returns either when all
// functions have terminated with a return value of false; or when one or more
// functions return true, without waiting for the other functions to terminate.
//
// If one or more thunks panic, the corresponding goroutines recover the panics,
// and Do may eventually panic with the left-most recovered panic value.
func Do(thunks ...func() bool) bool {
	switch len(thunks) {
	case 0:
		return false
	case 1:
		return thunks[0]()
	}
	var b0, b1 bool
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
			b1 = thunks[1]()
		}()
		b0 = thunks[0]()
	default:
		half := len(thunks) / 2
		go func() {
			defer func() {
				p = internal.WrapPanic(recover())
				wg.Done()
			}()
			b1 = Do(thunks[half:]...)
		}()
		b0 = Do(thunks[:half]...)
	}
	if b0 {
		return true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

// And receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and And returns true if all
// of them return true; or And returns false when at least one of them returns
// false, without waiting for the other predicates to terminate.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and And may eventually panic with the left-most recovered panic
// value.
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
	if !b0 {
		return false
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

// Or receives zero or more predicate functions and executes them in parallel.
//
// Each predicate is invoked in its own goroutine, and Or returns false if all
// of them return false; or Or returns true when at least one of them returns
// true, without waiting for the other predicates to terminate.
//
// If one or more predicates panic, the corresponding goroutines recover the
// panics, and Or may eventually panic with the left-most recovered panic value.
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
	if b0 {
		return true
	}
	wg.Wait()
	if p != nil {
		panic(p)
	}
	return b1
}

// Range receives a range and a range function f, divides the
// range into batches, and invokes the range function for each of these batches
// in parallel, covering the half-open interval from low to high, including low
// but excluding high.
//
// The range is specified by a low and high integer, with low <= high.
//
// The range function is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and Range returns either when all range functions have
// terminated with a return value of true; or when one or more range functions
// return true, without waiting for the other range functions to terminate.
//
// Range panics if high < low.
//
// If one or more range functions panic, the corresponding goroutines recover
// the panics, and Range may eventually panic with the left-most recovered panic
// value. If both non-nil error values are returned and panics occur, then the
// left-most of these events take precedence.
func Range(low, high int, f func(low, high int) bool) bool {
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
			var b1 bool
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
			if recur(low, mid, half) {
				return true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high))
}

// RangeAnd receives a range and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches in parallel, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high.
//
// The range predicate is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeAnd returns true if all of them return true; or
// RangeAnd returns false when at least one of them returns false, without
// waiting for the other range predicates to terminate.
//
// RangeAnd panics if high < low.
//
// If one or more range predicates panic, the corresponding goroutines recover
// the panics, and RangeAnd may eventually panic with the left-most recovered
// panic value.
func RangeAnd(low, high int, f func(low, high int) bool) bool {
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
			var b1 bool
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
			if !recur(low, mid, half) {
				return false
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high))
}

// RangeOr receives a range and a range predicate function f,
// divides the range into batches, and invokes the range predicate for each of
// these batches in parallel, covering the half-open interval from low to high,
// including low but excluding high.
//
// The range is specified by a low and high integer, with low <= high.
//
// The range predicate is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeOr returns false if all of them return false; or
// RangeOr returns true when at least one of them returns true, without waiting
// for the other range predicates to terminate.
//
// RangeOr panics if high < low.
//
// If one or more range predicates panic, the corresponding goroutines recover
// the panics, and RangeOr may eventually panic with the left-most recovered
// panic value.
func RangeOr(low, high int, f func(low, high int) bool) bool {
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
			var b1 bool
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
			if recur(low, mid, half) {
				return true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			return b1
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high))
}

// RangeReduce receives a range, a range reducer function, and
// a join function, divides the range into batches, and invokes the range
// reducer for each of these batches in parallel, covering the half-open
// interval from low to high, including low but excluding high. The results of
// the range reducer invocations are then combined by repeated invocations of
// join.
//
// The range is specified by a low and high integer, with low <= high.
//
// The range reducer is invoked for each batch in its own goroutine, with 0 <=
// low <= high, and RangeReduce returns either when all range reducers and joins
// have terminated with a second return value of false; or when one or more
// range or join functions return a second return value of true. In the latter
// case, the first return value of the left-most function that returned true as
// a second return value becomes the final result, without waiting for the other
// range and pair reducers to terminate.
//
// RangeReduce panics if high < low.
//
// If one or more reducer invocations panic, the corresponding goroutines
// recover the panics, and RangeReduce eventually panics with the left-most
// recovered panic value.
func RangeReduce[T any](
	low, high int,
	reduce func(low, high int) (T, bool),
	join func(x, y T) (T, bool),
) (T, bool) {
	var recur func(int, int, int) (T, bool)
	recur = func(low, high, n int) (T, bool) {
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
			var b0, b1 bool
			var p interface{}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer func() {
					p = internal.WrapPanic(recover())
					wg.Done()
				}()
				right, b1 = recur(mid, high, n-half)
			}()
			left, b0 = recur(low, mid, half)
			if b0 {
				return left, true
			}
			wg.Wait()
			if p != nil {
				panic(p)
			}
			if b1 {
				return right, true
			}
			return join(left, right)
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
	}
	return recur(low, high, internal.ComputeNofBatches(low, high))
}
