// Benchmarks comparing threaded vs async models for latency and p99 calculations in a simulated pipeline.
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::{Instant, Duration};
use std::thread;
use tokio::runtime::Runtime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use tokio::sync::{mpsc as tokio_mpsc, Mutex as TokioMutex};

// Constants for the benchmarks
const REQUESTS: usize = 50_000;
const QUEUE_CAPACITY: usize = 256;
const P99_WINDOW: usize = 1_000;
const JITTER_THRESHOLD_NS: u128 = 5_000_000;

// Helper function to compute the 99th percentile from a vector of latencies
fn compute_p99(mut data: Vec<u128>) -> u128 {
    data.sort();
    let idx = (data.len() as f64 * 0.99) as usize;
    data[idx]
}

// Simulated threaded model: Spawns multiple threads to process requests and collects latencies.
fn threaded_model() -> u128 {
    let latencies = Arc::new(Mutex::new(Vec::new()));

    let mut handles = vec![];

    for _ in 0..8 {
        let lat = Arc::clone(&latencies);

        handles.push(thread::spawn(move || {
            for _ in 0..(REQUESTS / 8) {
                let start = Instant::now();

                // Simulated work
                std::hint::black_box(1 + 1);

                let elapsed = start.elapsed().as_nanos();

                lat.lock().unwrap().push(elapsed);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    compute_p99(Arc::try_unwrap(latencies).unwrap().into_inner().unwrap())
}

// Simulated async model: Spawns multiple async tasks to process requests and collects latencies.
fn async_model() -> u128 {
    let rt = Runtime::new().unwrap();
    let latencies = Arc::new(Mutex::new(Vec::new()));

    rt.block_on(async {
        let mut handles = vec![];

        for _ in 0..REQUESTS {
            let lat = Arc::clone(&latencies);

            handles.push(tokio::spawn(async move {
                let start = Instant::now();

                // Simulated async work
                tokio::task::yield_now().await;

                let elapsed = start.elapsed().as_nanos();
                lat.lock().unwrap().push(elapsed);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    });

    compute_p99(Arc::try_unwrap(latencies).unwrap().into_inner().unwrap())
}

struct QueueItem {
    enqueued_at: Instant,
    is_bot: bool,
}

// Simualated threaded pipeline model
fn threaded_pipeline_p99() -> u128 {
    let (tx, rx) = mpsc::sync_channel::<QueueItem>(QUEUE_CAPACITY);
    let degraded = Arc::new(AtomicBool::new(false));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(REQUESTS)));

    let degraded_consumer = Arc::clone(&degraded);
    let latencies_consumer = Arc::clone(&latencies);

    let consumer = thread::spawn(move || {
        let mut window: Vec<u128> = Vec::with_capacity(P99_WINDOW);

        while let Ok(item) = rx.recv() {
            std::hint::black_box(1 + 1);

            let latency_ns = item.enqueued_at.elapsed().as_nanos();
            window.push(latency_ns);
            latencies_consumer.lock().unwrap().push(latency_ns);

            if window.len() == P99_WINDOW {
                let p99 = compute_p99(std::mem::take(&mut window));
                degraded_consumer.store(p99 > JITTER_THRESHOLD_NS, Ordering::Relaxed);
            }
        }

        if !window.is_empty() {
            let p99 = compute_p99(window);
            degraded_consumer.store(p99 > JITTER_THRESHOLD_NS, Ordering::Relaxed);
        }
    });

    let degraded_producer = Arc::clone(&degraded);
    for i in 0..REQUESTS {
        let is_bot = i % 2 == 0;
        if is_bot && degraded_producer.load(Ordering::Relaxed) {
            continue;
        }

        let item = QueueItem { enqueued_at: Instant::now(), is_bot };
        if tx.send(item).is_err() {
            break;
        }
    }

    drop(tx);
    consumer.join().unwrap();

    compute_p99(Arc::try_unwrap(latencies).unwrap().into_inner().unwrap())
}

// Simulated async pipeline model
fn async_pipeline_p99() -> u128 {
    let rt = Runtime::new().unwrap();
    let latencies = Arc::new(TokioMutex::new(Vec::with_capacity(REQUESTS)));
    let degraded = Arc::new(AtomicBool::new(false));

    rt.block_on(async {
        let (tx, mut rx) = tokio_mpsc::channel::<QueueItem>(QUEUE_CAPACITY);
        let latencies_consumer = Arc::clone(&latencies);
        let degraded_consumer = Arc::clone(&degraded);

        let consumer = tokio::spawn(async move {
            let mut window: Vec<u128> = Vec::with_capacity(P99_WINDOW);

            while let Some(item) = rx.recv().await {
                tokio::task::yield_now().await;

                let latency_ns = item.enqueued_at.elapsed().as_nanos();
                window.push(latency_ns);
                latencies_consumer.lock().await.push(latency_ns);

                if window.len() == P99_WINDOW {
                    let p99 = compute_p99(std::mem::take(&mut window));
                    degraded_consumer.store(p99 > JITTER_THRESHOLD_NS, Ordering::Relaxed);
                }
            }

            if !window.is_empty() {
                let p99 = compute_p99(window);
                degraded_consumer.store(p99 > JITTER_THRESHOLD_NS, Ordering::Relaxed);
            }
        });

        let degraded_producer = Arc::clone(&degraded);
        let tx_prod = tx.clone();
        let producer = tokio::spawn(async move {
            for i in 0..REQUESTS {
                let is_bot = i % 2 == 0;
                if is_bot && degraded_producer.load(Ordering::Relaxed) {
                    continue;
                }

                let item = QueueItem { enqueued_at: Instant::now(), is_bot };
                if tx_prod.send(item).await.is_err() {
                    break;
                }
            }
        });

        let _ = producer.await;
        drop(tx);
        let _ = consumer.await;
    });

    compute_p99(Arc::try_unwrap(latencies).unwrap().into_inner())
}

// Benchmarks for the threaded vs async p99 latency calculations.
fn architecture_benchmark(c: &mut Criterion) {
    c.bench_function("Threaded_p99", |b| {
        b.iter(|| threaded_model())
    });

    c.bench_function("Async_p99", |b| {
        b.iter(|| async_model())
    });
}

// Benchmarks for the threaded vs async pipeline p99 latency calculations.
fn pipeline_architecture_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Threaded vs Async Pipeline p99");
    group.bench_function("Threaded_pipeline_p99", |b| {
        b.iter(|| threaded_pipeline_p99())
    });

    group.bench_function("Async_pipeline_p99", |b| {
        b.iter(|| async_pipeline_p99())
    });
}

// Criterion group and main function to run the benchmarks
criterion_group!(benches, architecture_benchmark, pipeline_architecture_benchmark);
criterion_main!(benches);