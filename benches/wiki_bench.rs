use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rts_wiki::process_event;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn compare_architectures(c: &mut Criterion) {
    let raw_json = r#"{"server_name":"en.wikipedia.org","user":"RTS_User","bot":false}"#;
    let mut group = c.benchmark_group("Wikipedia_Processing");
    let leaderboard = Arc::new(Mutex::new(HashMap::<String, u64>::new()));

    // 1. Benchmark Architecture 1: Async/Tokio Logic
    group.bench_function("Async_Path", |b| {
        b.iter(|| {
            // In Async, we usually deal with String/Owned data from the channel
            let data = black_box(raw_json.to_string());
            let processing_start = black_box(Instant::now());
            process_event(&data, processing_start, &leaderboard)
        })
    });

    // 2. Benchmark Architecture 2: Multi-threaded Logic
    group.bench_function("Threaded_Path", |b| {
        b.iter(|| {
            // In Threaded, we measure the raw OS-thread processing speed
            let processing_start = black_box(Instant::now());
            process_event(black_box(raw_json), processing_start, &leaderboard)
        })
    });

    group.finish();
}

criterion_group!(benches, compare_architectures);
criterion_main!(benches);