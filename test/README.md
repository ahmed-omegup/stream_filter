# Stream Filter Service

A reactive data-delivery service that ingests change/events, matches them against live subscriptions, and pushes relevant updates to connected clients with low latency.

## Project Overview

**Purpose**: A reactive data-delivery service that ingests change/events, matches them against live subscriptions, and pushes relevant updates to connected clients with low latency.

**Core Idea**: Clients express interest as time/interval subscriptions ("customers" with [start, stop] intervals). Incoming events carry a timestamp. The service maintains an in-memory index to determine which subscriptions match each event and immediately forwards the matching customer IDs to a downstream router.

This repository contains implementations in multiple technologies to evaluate their performance characteristics and determine the best technology stack for production deployment.

## Architecture

```
Producer → Filter → Router
```

1. **Producer** - Generates events with timestamps and sends them to the filter
2. **Filter** - Maintains an interval tree of customer subscriptions, matches incoming events, forwards customer IDs
3. **Router** - Receives filtered customer IDs and logs processing statistics

## Implementations

This project evaluates two different technology stacks:

### Bun (TypeScript)
- **Location**: `filter/bun/`
- **Approach**: Queue-based processing with async/await patterns
- **Performance**: Optimized for high-throughput event processing

### Gleam (Functional)
- **Location**: `filter/gleam/stream_filter/`
- **Approach**: Actor-based processing with immutable data structures
- **Performance**: Fault-tolerant design with concurrent processing

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Git

### Running Performance Tests

#### Test Bun Implementation
```bash
# Start all services with Bun filter
docker compose --profile bun up
```

#### Test Gleam Implementation
```bash
# Clean up previous run
docker compose down

# Start all services with Gleam filter
docker compose --profile gleam up
```

#### High-Volume Testing
```bash
# Test with 1 million events
echo "EVENT_COUNT=1000000" > .env

# Test Bun
docker compose --profile bun up

# Clean up and test Gleam
docker compose down
docker compose --profile gleam up
```

## Configuration

### Key Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `EVENT_COUNT` | 100000 | Number of events to generate for testing |
| `NUM_CUSTOMERS` | 100000 | Number of customer subscriptions in interval tree |
| `MAX_DATE` | 100000 | Maximum timestamp range for events |
| `MAX_SPAN` | 10 | Maximum interval span for customer subscriptions |
| `MSS` | 1400 | TCP Maximum Segment Size for network optimization |

### Performance Tuning Examples

#### High Throughput Test
```env
EVENT_COUNT=2000000
NUM_CUSTOMERS=50000
MSS=1400
```

#### Memory Efficient Test
```env
EVENT_COUNT=100000
NUM_CUSTOMERS=10000
MAX_SPLITS=20
```

## Performance Comparison

### What to Monitor

1. **Throughput** - Events processed per second
2. **Memory Usage** - Peak memory consumption during processing
3. **Latency** - End-to-end processing time
4. **Resource Efficiency** - CPU and memory utilization

### Monitoring Commands

```bash
# View processing logs
docker compose logs -f

# Monitor resource usage
docker stats

# View specific implementation logs
docker compose logs -f filter-bun    # For Bun
docker compose logs -f filter-gleam  # For Gleam
```

## Local Development

### Running Components Individually

#### Bun Filter (Local)
```bash
cd filter/bun
bun install
bun index.ts
```

#### Gleam Filter (Local)
```bash
cd filter/gleam/stream_filter
gleam deps download
gleam build
gleam run
```

#### Router
```bash
cd router
bun install
bun index.ts
```

#### Producer
```bash
cd producer
EVENT_COUNT=1000 bun index.ts
```

## Troubleshooting

### Common Issues

#### Container Name Conflicts
```bash
docker compose down
docker rm $(docker ps -aq --filter "name=stream_filter")
```

#### Build Issues
```bash
# Clean rebuild
docker compose build --no-cache
```

#### Memory Issues
```bash
# Reduce test size
echo "EVENT_COUNT=10000" > .env
```

### Debug Mode

Enable verbose logging:
```bash
DEBUG=true docker compose --profile bun up
```

## Technology Evaluation

This project serves as a practical comparison between:

- **Bun/TypeScript**: Modern JavaScript runtime with focus on performance
- **Gleam/Erlang**: Functional language with actor model and fault tolerance

The goal is to determine which technology stack provides the best combination of:
- Processing throughput
- Memory efficiency  
- Development productivity
- Operational reliability

Run the tests with different configurations to evaluate which implementation best suits your specific requirements.