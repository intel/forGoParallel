// This package provides functions and data structures for expressing parallel
// algorithms.
//
// It provides the following subpackages:
//
// forGoParallel/parallel provides simple functions for executing series of thunks or
// predicates, as well as thunks, predicates, or reducers over ranges in
// parallel.
//
// forGoParallel/speculative provides speculative implementations of most of the
// functions from forGoParallel/parallel. These implementations not only execute in
// parallel, but also attempt to terminate early as soon as the final result is
// known.
//
// forGoParallel/psort provides parallel sorting algorithms.
//
// forGoParallel/gsync provides synchronization abstractions.
//
// forGoParallel/pipeline provides functions and data structures to construct and
// execute parallel pipelines.
package forGoParallel
