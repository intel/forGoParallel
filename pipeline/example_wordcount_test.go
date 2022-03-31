package pipeline_test

import (
	"bufio"
	"fmt"
	"github.com/intel/forGoParallel/gsync"
	"github.com/intel/forGoParallel/pipeline"
	"github.com/intel/forGoParallel/psort"
	"io"
	"strings"
	"sync/atomic"
)

func WordCount(r io.Reader) *gsync.Map[string, *int64] {
	var result gsync.Map[string, *int64]
	scanner := pipeline.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var p pipeline.Pipeline[[]string]
	p.Source(scanner)
	p.Add(
		pipeline.Par(pipeline.Receive(
			func(_ int, data []string) []string {
				var uniqueWords []string
				for _, s := range data {
					count := int64(1)
					if actual, loaded := result.LoadOrStore(s, &count); loaded {
						atomic.AddInt64(actual, 1)
					} else {
						uniqueWords = append(uniqueWords, s)
					}
				}
				return uniqueWords
			},
		)),
	)
	p.Run()
	if err := p.Err(); err != nil {
		panic(err)
	}
	return &result
}

func Example_wordCount() {
	r := strings.NewReader("The big black bug bit the big black bear but the big black bear bit the big black bug back")
	counts := WordCount(r)
	words := make(psort.StringSlice, 0)
	counts.Range(func(key string, _ *int64) bool {
		words = append(words, key)
		return true
	})
	psort.Sort(words)
	for _, word := range words {
		count, _ := counts.Load(word)
		fmt.Println(word, *count)
	}

	// Output:
	// The 1
	// back 1
	// bear 2
	// big 4
	// bit 2
	// black 4
	// bug 2
	// but 1
	// the 3
}

func WordCount2(text []string) *gsync.Map[string, *int64] {
	var result gsync.Map[string, *int64]
	var p pipeline.Pipeline[[]string]
	p.Source(pipeline.NewSlice(text))
	p.Add(
		pipeline.Par(pipeline.Receive(
			func(_ int, data []string) []string {
				var uniqueWords []string
				for _, s := range data {
					count := int64(1)
					if actual, loaded := result.LoadOrStore(s, &count); loaded {
						atomic.AddInt64(actual, 1)
					} else {
						uniqueWords = append(uniqueWords, s)
					}
				}
				return uniqueWords
			},
		)),
	)
	p.Run()
	return &result
}

func Example_wordCount2() {
	sentence := []string{"The", "big", "black", "bug", "bit", "the", "big", "black", "bear", "but", "the", "big", "black", "bear", "bit", "the", "big", "black", "bug", "back"}
	counts := WordCount2(sentence)
	words := make(psort.StringSlice, 0)
	counts.Range(func(key string, _ *int64) bool {
		words = append(words, key)
		return true
	})
	psort.StableSort(words)
	for _, word := range words {
		count, _ := counts.Load(word)
		fmt.Println(word, *count)
	}

	// Output:
	// The 1
	// back 1
	// bear 2
	// big 4
	// bit 2
	// black 4
	// bug 2
	// but 1
	// the 3
}
