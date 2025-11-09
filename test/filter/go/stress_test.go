package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func BenchmarkRealWorldScenario(b *testing.B) {
	// Real-world parameters
	numCustomers := 100000
	maxDate := 100000
	maxSpan := 10

	// Create tree
	tree := NewTreeNode(1, maxDate)

	// Generate customers (same as production)
	rand.Seed(42)
	customers := make([]Customer, numCustomers)
	for i := 0; i < numCustomers; i++ {
		start := rand.Intn(maxDate-maxSpan) + 1
		span := rand.Intn(maxSpan) + 1
		customers[i] = Customer{
			ID:    i + 1,
			Start: start,
			End:   start + span,
		}
	}

	// Insert all customers
	start := time.Now()
	for _, customer := range customers {
		tree.Insert(customer)
	}
	insertTime := time.Since(start)

	// Generate events to dispatch
	events := make([]int, b.N)
	rand.Seed(123)
	for i := 0; i < b.N; i++ {
		events[i] = rand.Intn(maxDate) + 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	matchCount := 0
	
	// Benchmark dispatch operations
	for i := 0; i < b.N; i++ {
		tree.Dispatch(events[i], func(customerID int) {
			matchCount++
		})
	}

	b.StopTimer()
	
	// Report stats
	nodes, totalCustomers := tree.Stats()
	fmt.Printf("\nInsert time: %v\n", insertTime)
	fmt.Printf("Tree nodes: %d\n", nodes)
	fmt.Printf("Total customer entries: %d\n", totalCustomers)
	fmt.Printf("Matches per event: %.2f\n", float64(matchCount)/float64(b.N))
}

func BenchmarkMemoryUsage(b *testing.B) {
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Create tree with production parameters
	tree := NewTreeNode(1, 100000)
	
	// Generate and insert customers
	rand.Seed(42)
	for i := 0; i < 100000; i++ {
		start := rand.Intn(90000) + 1
		span := rand.Intn(10) + 1
		customer := Customer{
			ID:    i + 1,
			Start: start,
			End:   start + span,
		}
		tree.Insert(customer)
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	fmt.Printf("\nMemory used by tree: %d KB\n", (m2.Alloc-m1.Alloc)/1024)
	fmt.Printf("Total allocations: %d\n", m2.TotalAlloc-m1.TotalAlloc)
	
	// Benchmark dispatch with memory tracking
	b.ReportAllocs()
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		date := rand.Intn(100000) + 1
		tree.Dispatch(date, func(customerID int) {
			// Do nothing, just count
		})
	}
}

func BenchmarkDispatchHotPath(b *testing.B) {
	// Focus on dispatch performance with pre-built tree
	tree := NewTreeNode(1, 100000)
	
	// Insert customers
	rand.Seed(42)
	for i := 0; i < 100000; i++ {
		start := rand.Intn(90000) + 1
		span := rand.Intn(10) + 1
		customer := Customer{
			ID:    i + 1,
			Start: start,
			End:   start + span,
		}
		tree.Insert(customer)
	}

	// Pre-generate dates to avoid rand overhead in benchmark
	dates := make([]int, b.N)
	rand.Seed(123)
	for i := 0; i < b.N; i++ {
		dates[i] = rand.Intn(100000) + 1
	}

	matchCount := 0
	callback := func(customerID int) {
		matchCount++
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tree.Dispatch(dates[i], callback)
	}

	b.StopTimer()
	fmt.Printf("\nTotal matches: %d\n", matchCount)
	fmt.Printf("Avg matches per dispatch: %.2f\n", float64(matchCount)/float64(b.N))
}

func BenchmarkInsertPerformance(b *testing.B) {
	trees := make([]*TreeNode, b.N)
	customers := make([]Customer, b.N)
	
	// Pre-generate data
	rand.Seed(42)
	for i := 0; i < b.N; i++ {
		trees[i] = NewTreeNode(1, 100000)
		start := rand.Intn(90000) + 1
		span := rand.Intn(10) + 1
		customers[i] = Customer{
			ID:    i + 1,
			Start: start,
			End:   start + span,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		trees[i].Insert(customers[i])
	}
}
