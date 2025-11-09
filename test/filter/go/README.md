# Go Interval Tree Implementation

This is a single-threaded Go implementation of the interval tree from the Gleam version, without the process/actor complexity.

## Files

- `main.go` - Main server that handles TCP connections and routes events
- `tree.go` - Interval tree implementation with binary tree node allocation  
- `tree_test.go` - Tests and benchmarks for the interval tree
- `go.mod` - Go module definition

## Key Features

- **Single-threaded**: No goroutines or channels, just direct function calls
- **Direct node references**: Uses normal Go pointers instead of map-based storage
- **Mutable data structures**: Takes advantage of Go's mutable slices and structs
- **Split threshold**: Automatically splits nodes when they exceed 10 customers
- **Efficient dispatch**: O(log n) tree traversal for customer lookups
- **Simple structure**: Mirrors TypeScript implementation closely

## Usage

### Build and run
```bash
go build .
./stream_filter
```

### Environment variables
- `ROUTER_HOST` - Router hostname (default: 127.0.0.1)
- `ROUTER_PORT` - Router port (default: 8000)  
- `NUM_CUSTOMERS` - Number of customers to generate (default: 100,000)
- `MAX_DATE` - Maximum date range (default: 100,000)
- `MAX_SPAN` - Maximum customer span (default: 10)

### Run tests
```bash
go test -v
```

### Run benchmarks
```bash
go test -bench=.
```

## Architecture

The tree structure mirrors the TypeScript implementation:

- **TreeNode** with direct Left/Right pointers
- **Mutable slices** for FullCustomers and PartialCustomers
- **Split when threshold exceeded** (>10 customers in a node)
- **Full customers** are stored separately (span entire node range)  
- **Partial customers** are stored in leaf nodes
- **Direct recursion** for tree traversal (no map lookups)

## Performance

On Intel i7-8750H:
- Insert: ~741 ns/op
- Dispatch: ~75 ns/op

The direct reference design eliminates all map lookup overhead and is much simpler than the ETS-based approach, while being significantly faster than process/actor patterns.
