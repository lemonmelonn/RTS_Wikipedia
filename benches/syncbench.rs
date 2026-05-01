// Benchmarks for synchronization primitives: Mutex, RwLock, and AtomicUsize.
use criterion::{criterion_group, criterion_main, Criterion, black_box};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

// Constants for the benchmarks
const THREADS: usize = 16;
const ITERATIONS: usize = 100_000;

// Benchmark for Mutex
// Spawns multiple threads that increment a shared counter protected by a Mutex.
fn bench_mutex() {
    let counter = Arc::new(Mutex::new(0usize));

    let mut handles = vec![];
    for _ in 0..THREADS {
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..ITERATIONS {
                let mut num = c.lock().unwrap();
                *num += 1;
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// Benchmark for RwLock
// Spawns multiple threads that increment a shared counter protected by a RwLock.
fn bench_rwlock() {
    let counter = Arc::new(RwLock::new(0usize));

    let mut handles = vec![];
    for _ in 0..THREADS {
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..ITERATIONS {
                let mut num = c.write().unwrap();
                *num += 1;
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// Benchmark for AtomicUsize
// Spawns multiple threads that increment a shared counter using atomic operations.
fn bench_atomic() {
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..THREADS {
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..ITERATIONS {
                c.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// Criterion benchmark function that runs all the synchronization primitive benchmarks.
fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Synchronization Primitives");

    group.bench_function("Mutex", |b| b.iter(|| bench_mutex()));
    group.bench_function("RwLock", |b| b.iter(|| bench_rwlock()));
    group.bench_function("Atomic", |b| b.iter(|| bench_atomic()));
}

// Criterion group and main function to run the benchmarks
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);