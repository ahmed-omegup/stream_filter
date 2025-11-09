package main

import (
	"fmt"
	"testing"
)

func TestIntervalTree(t *testing.T) {
	// Create a small tree
	tree := NewTreeNode(1, 100)
	
	// Add some customers
	customers := []Customer{
		{ID: 1, Start: 10, End: 20},
		{ID: 2, Start: 15, End: 25},
		{ID: 3, Start: 30, End: 40},
		{ID: 4, Start: 5, End: 35},   // Spans multiple ranges
		{ID: 5, Start: 1, End: 100}, // Spans entire tree
	}
	
	for _, customer := range customers {
		tree.Insert(customer)
	}
	
	// Test dispatch at various dates
	testCases := []struct {
		date     int
		expected []int
	}{
		{date: 12, expected: []int{1, 4, 5}}, // Should find customers 1, 4 and 5
		{date: 18, expected: []int{1, 2, 4, 5}}, // Should find customers 1, 2, 4, and 5
		{date: 32, expected: []int{3, 4, 5}}, // Should find customers 3, 4, and 5
		{date: 50, expected: []int{5}}, // Should find only customer 5
	}
	
	for _, tc := range testCases {
		var found []int
		tree.Dispatch(tc.date, func(customerID int) {
			found = append(found, customerID)
		})
		
		fmt.Printf("Date %d: found customers %v (expected %v)\n", 
			tc.date, found, tc.expected)
		
		// Check if we found the expected customers (order doesn't matter)
		if len(found) != len(tc.expected) {
			t.Errorf("Date %d: expected %d customers, got %d", 
				tc.date, len(tc.expected), len(found))
		} else {
			// Check that all expected customers are found
			expectedMap := make(map[int]bool)
			for _, id := range tc.expected {
				expectedMap[id] = true
			}
			for _, id := range found {
				if !expectedMap[id] {
					t.Errorf("Date %d: unexpected customer %d found", tc.date, id)
				}
			}
		}
	}
	
	// Print tree structure
	fmt.Println("\nTree structure:")
	tree.PrintTree(0)
	
	// Print stats
	nodes, totalCustomers := tree.Stats()
	fmt.Printf("\nStats: %d nodes, %d total customer entries\n", nodes, totalCustomers)
}

func TestTreeSplitting(t *testing.T) {
	// Create a tree and add enough customers to force splitting
	tree := NewTreeNode(1, 100)
	
	// Add more than splitThreshold customers to force a split
	for i := 1; i <= 15; i++ {
		customer := Customer{
			ID:    i,
			Start: i * 5,
			End:   i*5 + 3,
		}
		tree.Insert(customer)
	}
	
	fmt.Println("Tree after forcing split:")
	tree.PrintTree(0)
	
	// Test that dispatch still works correctly
	var found []int
	tree.Dispatch(25, func(customerID int) {
		found = append(found, customerID)
	})
	
	fmt.Printf("Found customers at date 25: %v\n", found)
}

func generateCustomersForTest(n, maxDate, maxSpan int) []Customer {
	customers := make([]Customer, n)
	
	for i := 0; i < n; i++ {
		start := (i*7)%(maxDate-maxSpan) + 1  // Use deterministic values for testing
		span := (i%maxSpan) + 1
		customers[i] = Customer{
			ID:    i + 1,
			Start: start,
			End:   start + span,
		}
	}
	
	return customers
}

func BenchmarkInsert(b *testing.B) {
	tree := NewTreeNode(1, 100000)
	customers := generateCustomersForTest(b.N, 100000, 10)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Insert(customers[i])
	}
}

func BenchmarkDispatch(b *testing.B) {
	tree := NewTreeNode(1, 100000)
	customers := generateCustomersForTest(10000, 100000, 10)
	
	// Insert customers
	for _, customer := range customers {
		tree.Insert(customer)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Dispatch(i%100000, func(customerID int) {
			// Do nothing, just benchmark the search
		})
	}
}
