package pipeline

import (
	"bufio"
	"context"
	"io"
)

// A Source represents an object that can generate data batches for pipelines.
type Source[T any] interface {
	// Err returns an error value or nil
	Err() error

	// Prepare receives a pipeline context and informs the pipeline what the
	// total expected size of all data batches is. The return value is -1 if the
	// total size is unknown or difficult to determine.
	Prepare(ctx context.Context) (size int)

	// Fetch gets a data batch of the requested size from the source. It returns
	// the size of the data batch that it was actually able to fetch. It returns
	// 0 if there is no more data to be fetched from the source; the pipeline
	// will then make no further attempts to fetch more elements.
	Fetch(size int) (fetched int)

	// Data returns the last fetched data batch.
	Data() T
}

type Slice[T any] struct {
	value []T
	size  int
	data  []T
}

func NewSlice[T any](value []T) *Slice[T] {
	size := len(value)
	return &Slice[T]{value: value, size: size}
}

func (src *Slice[T]) Err() error {
	return nil
}

func (src *Slice[T]) Prepare(_ context.Context) int {
	return src.size
}

func (src *Slice[T]) Fetch(n int) (fetched int) {
	switch {
	case src.size == 0:
		src.data = nil
	case n >= src.size:
		fetched = src.size
		src.data = src.value
		src.value = nil
		src.size = 0
	default:
		fetched = n
		src.data = src.value[:n]
		src.value = src.value[n:]
		src.size -= n
	}
	return
}

func (src *Slice[T]) Data() []T {
	return src.data
}

// Scanner is a wrapper around bufio.Scanner so it can act as a data source for
// pipelines. It fetches strings.
type Scanner struct {
	*bufio.Scanner
	data []string
}

// NewScanner returns a new Scanner to read from r. The split function defaults
// to bufio.ScanLines.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{Scanner: bufio.NewScanner(r)}
}

// Prepare implements the method of the Source interface.
func (src *Scanner) Prepare(_ context.Context) (size int) {
	return -1
}

// Fetch implements the method of the Source interface.
func (src *Scanner) Fetch(n int) (fetched int) {
	var data []string
	for fetched = 0; fetched < n; fetched++ {
		if src.Scan() {
			data = append(data, src.Text())
		} else {
			break
		}
	}
	src.data = data
	return
}

// Data implements the method of the Source interface.
func (src *Scanner) Data() []string {
	return src.data
}

// BytesScanner is a wrapper around bufio.Scanner so it can act as a data source
// for pipelines. It fetches slices of bytes.
type BytesScanner struct {
	*bufio.Scanner
	data [][]byte
}

// NewBytesScanner returns a new Scanner to read from r. The split function
// defaults to bufio.ScanLines.
func NewBytesScanner(r io.Reader) *BytesScanner {
	return &BytesScanner{Scanner: bufio.NewScanner(r)}
}

// Prepare implements the method of the Source interface.
func (src *BytesScanner) Prepare(_ context.Context) (size int) {
	return -1
}

// Fetch implements the method of the Source interface.
func (src *BytesScanner) Fetch(n int) (fetched int) {
	var data [][]byte
	for fetched = 0; fetched < n; fetched++ {
		if src.Scan() {
			data = append(data, append([]byte(nil), src.Bytes()...))
		} else {
			break
		}
	}
	src.data = data
	return
}

// Data implements the method of the Source interface.
func (src *BytesScanner) Data() [][]byte {
	return src.data
}

// Func is a generic source that generates data batches
// by repeatedly calling a function.
type Func[T any] struct {
	data  T
	err   error
	size  int
	fetch func(size int) (data T, fetched int, err error)
}

// NewFunc returns a new Func to generate data batches
// by repeatedly calling fetch.
//
// The size parameter informs the pipeline what the total
// expected size of all data batches is. Pass -1 if the
// total size is unknown or difficult to determine.
//
// The fetch function returns a data batch of the requested
// size. It returns the size of the data batch that it was
// actually able to fetch. It returns 0 if there is no more
// data to be fetched from the source; the pipeline will
// then make no further attempts to fetch more elements.
//
// The fetch function can also return an error if necessary.
func NewFunc[T any](size int, fetch func(size int) (data T, fetched int, err error)) *Func[T] {
	return &Func[T]{size: size, fetch: fetch}
}

// Err implements the method of the Source interface.
func (f *Func[T]) Err() error {
	return f.err
}

// Prepare implements the method of the Source interface.
func (f *Func[T]) Prepare(_ context.Context) int {
	return f.size
}

// Fetch implements the method of the Source interface.
func (f *Func[T]) Fetch(size int) (fetched int) {
	f.data, fetched, f.err = f.fetch(size)
	return
}

// Data implements the method of the Source interface.
func (f *Func[T]) Data() T {
	return f.data
}

// Chan is a source, that accepts and passes through
// single elements from the input channel.
type Chan[T any] struct {
	channel <-chan T
	ctx     context.Context
	data    T
}

// NewChan returns a new Chan to read from
// the given channel.
func NewChan[T any](channel <-chan T) *Chan[T] {
	return &Chan[T]{
		channel: channel,
	}
}

// Err implements the method of the Source interface.
func (src *Chan[T]) Err() error {
	return nil
}

// Prepare implements the method of the Source interface.
func (src *Chan[T]) Prepare(ctx context.Context) (size int) {
	src.ctx = ctx
	return -1
}

// Fetch implements the method of the Source interface.
func (src *Chan[T]) Fetch(n int) (fetched int) {
	select {
	case element, ok := <-src.channel:
		if ok {
			src.data = element
			return 1
		}
	case <-src.ctx.Done():
	}
	var t T
	src.data = t
	return 0
}

// Data implements the method of the Source interface.
func (src *Chan[T]) Data() T {
	return src.data
}

// MultiChan is a source, that accepts and passes through
// multiple elements from the input channel.
type MultiChan[T any] struct {
	channel <-chan T
	ctx     context.Context
	data    []T
}

// NewMultiChan returns a new MultiChan to read from
// the given channel.
func NewMultiChan[T any](channel <-chan T) *MultiChan[T] {
	return &MultiChan[T]{
		channel: channel,
	}
}

// Err implements the method of the Source interface.
func (src *MultiChan[T]) Err() error {
	return nil
}

// Prepare implements the method of the Source interface.
func (src *MultiChan[T]) Prepare(ctx context.Context) (size int) {
	src.ctx = ctx
	return -1
}

// Fetch implements the method of the Source interface.
func (src *MultiChan[T]) Fetch(n int) (fetched int) {
	src.data = nil
	for fetched < n {
		select {
		case element, ok := <-src.channel:
			if ok {
				src.data = append(src.data, element)
				fetched++
			} else {
				return
			}
		case <-src.ctx.Done():
			src.data = nil
			return 0
		}
	}
	return
}

// Data implements the method of the Source interface.
func (src *MultiChan[T]) Data() []T {
	return src.data
}
