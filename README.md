# RTS Wikipedia

Individual assignment for "Real Time Systems" module at APU. (Degree)
A real-time systems project in Rust that ingests the live Wikipedia edit stream and compares two processing architectures:

- **Async** (Tokio) – `src/bin/async_pipeline.rs`
- **Multi-threaded** (`std::thread`) – `src/bin/threaded_pipeline.rs`

Both use zero-copy parsing of edit events, bounded channels with drop-oldest backpressure, and a 2 ms processing deadline. Scheduling drift, jitter and tail latency (p50/p90/p99) are measured and benchmarked with Criterion (`benches/`) to determine which architecture handles high-velocity spikes better.

## Run

```bash
cargo run --release --bin async_pipeline
cargo run --release --bin threaded_pipeline
cargo bench
```