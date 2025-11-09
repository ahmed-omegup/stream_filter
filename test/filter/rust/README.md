# Rust Interval Tree Implementation

This is a Rust implementation of the interval tree using LinkedList structures for flexible data management.

## Files

- `src/main.rs` - Main server that handles TCP connections and routes events
- `src/tree.rs` - Interval tree implementation with LinkedList collections
- `Cargo.toml` - Rust package definition

## Key Features

- **LinkedList collections**: Uses `std::collections::LinkedList` for both full and partial customers
- **Zero-copy insertions**: O(1) push_back operations for adding customers
- **Memory efficient**: No pre-allocation overhead, grows dynamically
- **Safe memory management**: Rust's ownership system prevents memory leaks
- **Thread-safe**: Uses Arc and Mutex for shared access across threads

## Usage

### Build and run
```bash
cargo build --release
./target/release/stream_filter
```

### Environment variables
- `ROUTER_HOST` - Router hostname (default: 127.0.0.1)
- `ROUTER_PORT` - Router port (default: 8000)
- `NUM_CUSTOMERS` - Number of customers to generate (default: 100,000)
- `MAX_DATE` - Maximum date range (default: 100,000)
- `MAX_SPAN` - Maximum customer span (default: 10)

### Run tests
```bash
cargo test
```

### Run tests with output
```bash
cargo test -- --nocapture
```

## Architecture

The tree structure uses Rust's LinkedList for flexible data management:

- **TreeNode** with Box<TreeNode> for owned child pointers
- **LinkedList<Customer>** for both FullCustomers and PartialCustomers
- **Split when threshold exceeded** (>10 customers in a node)
- **Memory safety** ensured by Rust's borrow checker
- **Thread safety** via Arc and Mutex wrappers

## Performance Characteristics

- **Insert**: O(1) for linked list operations, O(log n) for tree traversal
- **Dispatch**: O(n) for linked list iteration, O(log n) for tree traversal
- **Memory**: Dynamic allocation, no waste from pre-allocated capacity
- **Safety**: Zero undefined behavior, no memory leaks

## Comparison with Go Implementation

| Aspect | Go (LinkedList) | Rust (LinkedList) |
|--------|-----------------|-------------------|
| Memory Safety | Runtime panics | Compile-time guarantees |
| Data Structure | container/list | std::collections::LinkedList |
| Threading | Goroutines + channels | Threads + Arc/Mutex |
| Performance | Interface{} boxing overhead | Zero-cost abstractions |
| Development | Simple syntax | Strong type system |
