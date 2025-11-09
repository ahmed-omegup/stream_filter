package main

import (
	"fmt"
	"container/list"
)

const splitThreshold = 10

// Customer represents an interval customer
type Customer struct {
	ID    int
	Start int
	End   int // Changed from Stop to End to match TypeScript
}

// TreeNode represents a tree node with linked lists
type TreeNode struct {
	Min              int
	Max              int
	Mid              int
	FullCustomers    *list.List     // Linked list for O(1) insertions
	PartialCustomers *list.List     // Linked list for O(1) insertions
	Left             *TreeNode
	Right            *TreeNode
}

// NewTreeNode creates a new tree node with pre-allocated slices
func NewTreeNode(min, max int) *TreeNode {
	return &TreeNode{
		Min:              min,
		Max:              max,
		Mid:              (min + max) / 2,
		FullCustomers:    list.New(),
		PartialCustomers: list.New(),
		Left:             nil,
		Right:            nil,
	}
}

// Insert inserts a customer into the tree node
func (node *TreeNode) Insert(customer Customer) {
	start, end := customer.Start, customer.End

	// Full containment
	if start <= node.Min && end >= node.Max {
		node.FullCustomers.PushBack(customer)
		return
	}

	// Partial overlap
	node.PartialCustomers.PushBack(customer)

	if node.PartialCustomers.Len() > splitThreshold && node.Min != node.Max {
		// Split
		if node.Left == nil && node.Right == nil {
			node.Left = NewTreeNode(node.Min, node.Mid)
			node.Right = NewTreeNode(node.Mid+1, node.Max)
		}

		// Reinsert all partial customers into children
		var next *list.Element
		for e := node.PartialCustomers.Front(); e != nil; e = next {
			next = e.Next()
			customer := e.Value.(Customer)
			node.insertIntoChildren(customer)
			node.PartialCustomers.Remove(e)
		}
	}
}

func (node *TreeNode) insertIntoChildren(customer Customer) {
	if customer.Start <= node.Mid {
		node.Left.Insert(customer)
	}
	if customer.End > node.Mid {
		node.Right.Insert(customer)
	}
}

// Dispatch finds all customers that match the given date and calls the function for each
func (node *TreeNode) Dispatch(date int, fn func(int)) {
	// Fast path: send to fullCustomers (linked list iteration)
	for e := node.FullCustomers.Front(); e != nil; e = e.Next() {
		customer := e.Value.(Customer)
		fn(customer.ID)
	}

	// Check partialCustomers (linked list iteration)
	for e := node.PartialCustomers.Front(); e != nil; e = e.Next() {
		customer := e.Value.(Customer)
		if customer.Start <= date && date <= customer.End {
			fn(customer.ID)
		}
	}

	// Traverse children
	if node.Left != nil && date <= node.Mid {
		node.Left.Dispatch(date, fn)
	} else if node.Right != nil && date > node.Mid {
		node.Right.Dispatch(date, fn)
	}
}

// PrintTree prints the tree structure for debugging
func (node *TreeNode) PrintTree(depth int) {
	indent := ""
	for i := 0; i < depth; i++ {
		indent += "  "
	}

	fmt.Printf("%sNode [%d,%d] mid:%d\n", indent, node.Min, node.Max, node.Mid)
	fmt.Printf("%s  Full: %d customers\n", indent, node.FullCustomers.Len())
	fmt.Printf("%s  Partial: %d customers\n", indent, node.PartialCustomers.Len())

	if node.Left != nil {
		fmt.Printf("%s  Left:\n", indent)
		node.Left.PrintTree(depth + 1)
	}
	if node.Right != nil {
		fmt.Printf("%s  Right:\n", indent)
		node.Right.PrintTree(depth + 1)
	}
}

// Stats returns statistics about the tree
func (node *TreeNode) Stats() (totalNodes, totalCustomers int) {
	totalNodes = 1
	totalCustomers = node.FullCustomers.Len() + node.PartialCustomers.Len()

	if node.Left != nil {
		leftNodes, leftCustomers := node.Left.Stats()
		totalNodes += leftNodes
		totalCustomers += leftCustomers
	}
	if node.Right != nil {
		rightNodes, rightCustomers := node.Right.Stats()
		totalNodes += rightNodes
		totalCustomers += rightCustomers
	}

	return totalNodes, totalCustomers
}
