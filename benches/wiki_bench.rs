use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use rts_wiki::{process_event, record_jitter, update_degraded_mode, RunStats, WikipediaEdit};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const JITTER_THRESHOLD_MS: u64 = 2;
const JITTER_WINDOW_SIZE: usize = 100;
const SPIKE_EVENTS: usize = 10_000;

struct BenchState {
    leaderboard: Arc<Mutex<HashMap<String, u64>>>,
    human_latencies: Arc<Mutex<Vec<Duration>>>,
    bot_latencies: Arc<Mutex<Vec<Duration>>>,
    human_jitters: Arc<Mutex<Vec<Duration>>>,
    bot_jitters: Arc<Mutex<Vec<Duration>>>,
    human_drifts: Arc<Mutex<Vec<Duration>>>,
    bot_drifts: Arc<Mutex<Vec<Duration>>>,
    run_stats: Arc<Mutex<RunStats>>,
    degraded_mode: AtomicBool,
}

impl BenchState {
    fn new() -> Self {
        Self {
            leaderboard: Arc::new(Mutex::new(HashMap::<String, u64>::new())),
            human_latencies: Arc::new(Mutex::new(Vec::<Duration>::new())),
            bot_latencies: Arc::new(Mutex::new(Vec::<Duration>::new())),
            human_jitters: Arc::new(Mutex::new(Vec::<Duration>::new())),
            bot_jitters: Arc::new(Mutex::new(Vec::<Duration>::new())),
            human_drifts: Arc::new(Mutex::new(Vec::<Duration>::new())),
            bot_drifts: Arc::new(Mutex::new(Vec::<Duration>::new())),
            run_stats: Arc::new(Mutex::new(RunStats::default())),
            degraded_mode: AtomicBool::new(false),
        }
    }
}

fn process_full_path(raw_json: &str, enqueue_time: Instant, state: &BenchState) {
    let dequeue_time = Instant::now();
    let drift = dequeue_time.duration_since(enqueue_time);
    let (violated, latency) = process_event(raw_json, dequeue_time, &state.leaderboard);

    if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(raw_json) {
        let drift_list = if edit.bot { &state.bot_drifts } else { &state.human_drifts };
        if let Ok(mut list) = drift_list.lock() {
            list.push(drift);
        }

        let jitter = if edit.bot {
            record_jitter(&state.bot_latencies, latency, JITTER_WINDOW_SIZE)
        } else {
            record_jitter(&state.human_latencies, latency, JITTER_WINDOW_SIZE)
        };

        if let Some(jitter) = jitter {
            update_degraded_mode(jitter, JITTER_THRESHOLD_MS, &state.degraded_mode);
            let jitter_list = if edit.bot { &state.bot_jitters } else { &state.human_jitters };
            if let Ok(mut list) = jitter_list.lock() {
                list.push(jitter);
            }
        }

        if let Ok(mut stats) = state.run_stats.lock() {
            stats.record(drift, latency, violated, jitter);
        }
    }
}

fn percentile_duration(samples: &[Duration], percentile: f64) -> Option<Duration> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort();
    let rank = ((percentile / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted.get(rank).copied()
}

fn combined_durations(human: &Arc<Mutex<Vec<Duration>>>, bot: &Arc<Mutex<Vec<Duration>>>) -> Vec<Duration> {
    let mut out = Vec::new();
    if let Ok(list) = human.lock() {
        out.extend(list.iter().copied());
    }
    if let Ok(list) = bot.lock() {
        out.extend(list.iter().copied());
    }
    out
}

fn run_spike_analysis(label: &str, use_owned: bool) {
    let state = BenchState::new();
    let human_json = r#"{"server_name":"en.wikipedia.org","user":"RTS_User","bot":false}"#;
    let bot_json = r#"{"server_name":"en.wikipedia.org","user":"RTS_Bot","bot":true}"#;

    for i in 0..SPIKE_EVENTS {
        let raw = if i % 2 == 0 { human_json } else { bot_json };
        let enqueue_time = Instant::now();
        if use_owned {
            let data = raw.to_string();
            process_full_path(&data, enqueue_time, &state);
        } else {
            process_full_path(raw, enqueue_time, &state);
        }
    }

    let latencies = combined_durations(&state.human_latencies, &state.bot_latencies);
    let jitters = combined_durations(&state.human_jitters, &state.bot_jitters);
    let drifts = combined_durations(&state.human_drifts, &state.bot_drifts);

    let latency_p99 = percentile_duration(&latencies, 99.0).unwrap_or(Duration::from_secs(0));
    let jitter_p99 = percentile_duration(&jitters, 99.0).unwrap_or(Duration::from_secs(0));
    let drift_p99 = percentile_duration(&drifts, 99.0).unwrap_or(Duration::from_secs(0));

    println!(
        "[SPIKE P99] {} | latency: {:?} | jitter: {:?} | drift: {:?} | samples: {}",
        label,
        latency_p99,
        jitter_p99,
        drift_p99,
        SPIKE_EVENTS
    );
}

fn compare_architectures(c: &mut Criterion) {
    let raw_json = r#"{"server_name":"en.wikipedia.org","user":"RTS_User","bot":false}"#;
    let mut group = c.benchmark_group("Wikipedia_Processing");

    run_spike_analysis("Async", true);
    run_spike_analysis("Threaded", false);

    group.bench_function("Async_Full_Path", |b| {
        b.iter_batched(
            BenchState::new,
            |state| {
                let data = black_box(raw_json.to_string());
                let enqueue_time = black_box(Instant::now());
                process_full_path(&data, enqueue_time, &state);
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("Threaded_Full_Path", |b| {
        b.iter_batched(
            BenchState::new,
            |state| {
                let enqueue_time = black_box(Instant::now());
                process_full_path(black_box(raw_json), enqueue_time, &state);
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("Async_Spike", |b| {
        b.iter_batched(
            BenchState::new,
            |state| {
                for i in 0..SPIKE_EVENTS {
                    let raw = if i % 2 == 0 {
                        r#"{"server_name":"en.wikipedia.org","user":"RTS_User","bot":false}"#
                    } else {
                        r#"{"server_name":"en.wikipedia.org","user":"RTS_Bot","bot":true}"#
                    };
                    let enqueue_time = black_box(Instant::now());
                    let data = raw.to_string();
                    process_full_path(&data, enqueue_time, &state);
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("Threaded_Spike", |b| {
        b.iter_batched(
            BenchState::new,
            |state| {
                for i in 0..SPIKE_EVENTS {
                    let raw = if i % 2 == 0 {
                        r#"{"server_name":"en.wikipedia.org","user":"RTS_User","bot":false}"#
                    } else {
                        r#"{"server_name":"en.wikipedia.org","user":"RTS_Bot","bot":true}"#
                    };
                    let enqueue_time = black_box(Instant::now());
                    process_full_path(black_box(raw), enqueue_time, &state);
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, compare_architectures);
criterion_main!(benches);