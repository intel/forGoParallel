package internal

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// ComputeNofBatches divides the size of the range (high - low) by n. If n is 0,
// a default is used that takes runtime.NumCPU() into account.
func ComputeNofBatches(low, high, n int) (batches int) {
	switch size := high - low; {
	case size > 0:
		switch {
		case n == 0:
			batches = 2 * runtime.NumCPU()
		case n > 0:
			batches = n
		default:
			panic(fmt.Sprintf("invalid number of batches: %v", n))
		}
		if batches > size {
			batches = size
		}
	case size == 0:
		batches = 1
	default:
		panic(fmt.Sprintf("invalid range: %v:%v", low, high))
	}
	return
}

// WrapPanic adds stack trace information to a recovered panic.
func WrapPanic(p interface{}) interface{} {
	if p != nil {
		if err, isError := p.(error); isError {
			return fmt.Errorf("%w\n%s\nrethrown at", err, debug.Stack())
		}
		return fmt.Sprintf("%v\n%s\nrethrown at", p, debug.Stack())
	}
	return nil
}
