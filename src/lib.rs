// src/lib.rs
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// Struct to represent a Wikipedia edit event, deserialized from JSON
#[derive(Deserialize, Debug)]
pub struct WikipediaEdit<'a> {
    #[serde(rename = "server_name")]
    pub server: &'a str, // Zero-copy string slice for server name
    pub user: &'a str, // Zero-copy string slice for user name
    pub bot: bool,
}

// Move the core analysis here so Criterion can call it
pub fn process_event(
    raw_json: &str,
    processing_start: Instant,
    leaderboard: &Arc<Mutex<HashMap<String, u64>>>,
) -> (bool, Duration) {
    if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(raw_json) {
        // Update leaderboard counts
        update_leaderboard(leaderboard, edit.server);

        // Compute latency and check for violation
        let latency = processing_start.elapsed();
        let violated = latency > Duration::from_millis(2);
        return (violated, latency);
    }
    (false, Duration::from_secs(0))
}

// Function to update the leaderboard counts for a given domain
fn update_leaderboard(leaderboard: &Arc<Mutex<HashMap<String, u64>>>, domain: &str) {
    if let Ok(mut map) = leaderboard.lock() {
        *map.entry(domain.to_string()).or_insert(0) += 1;
    }
}

// Function to record jitter based on a sliding window of recent latencies
pub fn record_jitter(
    latencies: &Arc<Mutex<Vec<Duration>>>,
    latency: Duration,
    window_size: usize,
) -> Option<Duration> {
    let Ok(mut list) = latencies.lock() else {
        return None;
    };

    list.push(latency);

    let total_len = list.len();
    if total_len < 2 {
        return None;
    }

    let window_len = total_len.min(window_size.max(2));
    let start = total_len - window_len;
    let window = &list[start..];

    let sum_ns: f64 = window.iter().map(|d| d.as_nanos() as f64).sum();
    let mean = sum_ns / window_len as f64;
    let variance = window
        .iter()
        .map(|d| {
            let v = d.as_nanos() as f64 - mean;
            v * v
        })
        .sum::<f64>()
        / window_len as f64;

    Some(Duration::from_nanos(variance.sqrt().round() as u64))
}

// Struct to hold running statistics for the current run
// Including counts, sums, and max values for drift, latency, and jitter
#[derive(Default)]
pub struct RunStats {
    pub total_packets: u64,
    pub total_violations: u64,
    pub drift_sum_ns: u128,
    pub latency_sum_ns: u128,
    pub jitter_sum_ns: u128,
    pub jitter_samples: u64,
    pub max_drift: Duration,
    pub max_latency: Duration,
    pub max_jitter: Duration,
}

// Method to record a new packet's stats into the running totals and update max values as needed
impl RunStats {
    pub fn record(&mut self, drift: Duration, latency: Duration, violated: bool, jitter: Option<Duration>) {
        self.total_packets += 1;
        self.drift_sum_ns += drift.as_nanos();
        self.latency_sum_ns += latency.as_nanos();
        if drift > self.max_drift {
            self.max_drift = drift;
        }
        if latency > self.max_latency {
            self.max_latency = latency;
        }
        if violated {
            self.total_violations += 1;
        }
        if let Some(j) = jitter {
            self.jitter_samples += 1;
            self.jitter_sum_ns += j.as_nanos();
            if j > self.max_jitter {
                self.max_jitter = j;
            }
        }
    }
}

// Helper function to compute average duration from total nanoseconds and count
fn avg_duration(sum_ns: u128, count: u64) -> Duration {
    if count == 0 {
        return Duration::from_secs(0);
    }
    Duration::from_nanos((sum_ns / count as u128) as u64)
}

// Helper function to compute the specified percentile from a list of durations
fn percentile_duration(samples: &[Duration], percentile: f64) -> Option<Duration> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort();
    let rank = ((percentile / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted.get(rank).copied()
}

// Function to print the final statistics at the end of the run
// Including total packets, violations, average/max latency, jitter, and drift for both human and bot packets
pub fn print_final_statistics(
    stats: &RunStats,
    human_latencies: &Arc<Mutex<Vec<Duration>>>,
    bot_latencies: &Arc<Mutex<Vec<Duration>>>,
    human_jitters: &Arc<Mutex<Vec<Duration>>>,
    bot_jitters: &Arc<Mutex<Vec<Duration>>>,
    human_drifts: &Arc<Mutex<Vec<Duration>>>,
    bot_drifts: &Arc<Mutex<Vec<Duration>>>,
) {
    if stats.total_packets == 0 {
        println!("\n[STATS] No packets processed.");
        return;
    }

    let human_count = human_latencies.lock().map(|v| v.len() as u64).unwrap_or(0);
    let bot_count = bot_latencies.lock().map(|v| v.len() as u64).unwrap_or(0);
    let human_jitter_count = human_jitters.lock().map(|v| v.len() as u64).unwrap_or(0);
    let bot_jitter_count = bot_jitters.lock().map(|v| v.len() as u64).unwrap_or(0);

    let max_human_drift = human_drifts
        .lock()
        .ok()
        .and_then(|v| v.iter().max().copied())
        .unwrap_or(Duration::from_secs(0));
    let max_bot_drift = bot_drifts
        .lock()
        .ok()
        .and_then(|v| v.iter().max().copied())
        .unwrap_or(Duration::from_secs(0));
    let human_avg_drift = avg_duration(
        human_drifts
            .lock()
            .map(|v| v.iter().map(|d| d.as_nanos()).sum())
            .unwrap_or(0),
        human_count,
    );
    let bot_avg_drift = avg_duration(
        bot_drifts
            .lock()
            .map(|v| v.iter().map(|d| d.as_nanos()).sum())
            .unwrap_or(0),
        bot_count,
    );

    let max_human_latency = human_latencies
        .lock()
        .ok()
        .and_then(|v| v.iter().max().copied())
        .unwrap_or(Duration::from_secs(0));
    let max_bot_latency = bot_latencies
        .lock()
        .ok()
        .and_then(|v| v.iter().max().copied())
        .unwrap_or(Duration::from_secs(0));
    let human_avg_latency = avg_duration(
        human_latencies
            .lock()
            .map(|v| v.iter().map(|d| d.as_nanos()).sum())
            .unwrap_or(0),
        human_count,
    );
    let bot_avg_latency = avg_duration(
        bot_latencies
            .lock()
            .map(|v| v.iter().map(|d| d.as_nanos()).sum())
            .unwrap_or(0),
        bot_count,
    );

    let max_human_jitter = human_jitters
        .lock()
        .ok()
        .and_then(|v| v.iter().max().copied())
        .unwrap_or(Duration::from_secs(0));
    let max_bot_jitter = bot_jitters
        .lock()
        .ok()
        .and_then(|v| v.iter().max().copied())
        .unwrap_or(Duration::from_secs(0));
    let human_avg_jitter = avg_duration(
        human_jitters
            .lock()
            .map(|v| v.iter().map(|d| d.as_nanos()).sum())
            .unwrap_or(0),
        human_jitter_count,
    );
    let bot_avg_jitter = avg_duration(
        bot_jitters
            .lock()
            .map(|v| v.iter().map(|d| d.as_nanos()).sum())
            .unwrap_or(0),
        bot_jitter_count,
    );

    let human_drift_percentiles = human_drifts.lock().ok().map(|v| {
        (
            percentile_duration(&v, 50.0),
            percentile_duration(&v, 90.0),
            percentile_duration(&v, 99.0),
        )
    });
    let bot_drift_percentiles = bot_drifts.lock().ok().map(|v| {
        (
            percentile_duration(&v, 50.0),
            percentile_duration(&v, 90.0),
            percentile_duration(&v, 99.0),
        )
    });

    let human_latency_percentiles = human_latencies.lock().ok().map(|v| {
        (
            percentile_duration(&v, 50.0),
            percentile_duration(&v, 90.0),
            percentile_duration(&v, 99.0),
        )
    });
    let bot_latency_percentiles = bot_latencies.lock().ok().map(|v| {
        (
            percentile_duration(&v, 50.0),
            percentile_duration(&v, 90.0),
            percentile_duration(&v, 99.0),
        )
    });
    
    let human_jitter_percentiles = human_jitters.lock().ok().map(|v| {
        (
            percentile_duration(&v, 50.0),
            percentile_duration(&v, 90.0),
            percentile_duration(&v, 99.0),
        )
    });
    let bot_jitter_percentiles = bot_jitters.lock().ok().map(|v| {
        (
            percentile_duration(&v, 50.0),
            percentile_duration(&v, 90.0),
            percentile_duration(&v, 99.0),
        )
    });

    let violation_rate = (stats.total_violations as f64 / stats.total_packets as f64) * 100.0;

    println!("\n==================== [FINAL STATISTICS] =====================");
    println!(
        "\n[STATS] Packets: {} | Violations: {} ({:.2}%)",
        stats.total_packets, stats.total_violations, violation_rate
    );

    println!("\n======================= [DRIFT STATS] =======================");
    println!("[STATS] Human Drift avg/max: {:?} / {:?}", human_avg_drift, max_human_drift);
    println!("[STATS] Bot Drift avg/max: {:?} / {:?}", bot_avg_drift, max_bot_drift);
    if let Some((p50, p90, p99)) = human_drift_percentiles {
        if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
            println!("[STATS] Human Drift p50/p90/p99: {:?} / {:?} / {:?}", p50, p90, p99);
        }
    }
    if let Some((p50, p90, p99)) = bot_drift_percentiles {
        if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
            println!("[STATS] Bot Drift p50/p90/p99: {:?} / {:?} / {:?}", p50, p90, p99);
        }
    }

    println!("\n======================= [LATENCY STATS] =======================");
    println!("[STATS] Human Latency avg/max: {:?} / {:?}", human_avg_latency, max_human_latency);
    println!("[STATS] Bot Latency avg/max: {:?} / {:?}", bot_avg_latency, max_bot_latency);
    if let Some((p50, p90, p99)) = human_latency_percentiles {
        if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
            println!("[STATS] Human Latency p50/p90/p99: {:?} / {:?} / {:?}", p50, p90, p99);
        }
    }
    if let Some((p50, p90, p99)) = bot_latency_percentiles {
        if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
            println!("[STATS] Bot Latency p50/p90/p99: {:?} / {:?} / {:?}", p50, p90, p99);
        }
    }

    if stats.jitter_samples > 0 {
        println!("\n======================= [JITTER STATS] =======================");
        println!("[STATS] Human Jitter avg/max (windowed): {:?} / {:?}", human_avg_jitter, max_human_jitter);
        println!("[STATS] Bot Jitter avg/max (windowed): {:?} / {:?}", bot_avg_jitter, max_bot_jitter);
        if let Some((p50, p90, p99)) = human_jitter_percentiles {
            if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
                println!("[STATS] Human Jitter p50/p90/p99: {:?} / {:?} / {:?}", p50, p90, p99);
            }
        }
        if let Some((p50, p90, p99)) = bot_jitter_percentiles {
            if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
                println!("[STATS] Bot Jitter p50/p90/p99: {:?} / {:?} / {:?}", p50, p90, p99);
            }
        }
    } else {
        println!("\n[STATS] Jitter: n/a");
    }
}

// Function to update degraded mode status based on jitter
pub fn update_degraded_mode(jitter: Duration, threshold_ms: u64, degraded: &AtomicBool) {
    let threshold = Duration::from_millis(threshold_ms);
    let should_degrade = jitter > threshold;
    let was_active = degraded.swap(should_degrade, Ordering::SeqCst);

    if should_degrade && !was_active {
        println!("[DEGRADED MODE] Jitter threshold exceeded. Prioritizing human packets.");
    } else if !should_degrade && was_active {
        println!("[RECOVERED] Jitter back under threshold. Bot packets resumed.");
    }
}

// Function to print the top 3 domains in the leaderboard, showing their edit counts
pub fn print_top_three(leaderboard: &Arc<Mutex<HashMap<String, u64>>>) {
    let Ok(map) = leaderboard.lock() else {
        return;
    };

    if map.is_empty() {
        println!("[LEADERBOARD] No data yet.");
        return;
    }

    println!("\n\n[LEADERBOARD] Top 3 Domains:");

    let mut items: Vec<_> = map.iter().collect();
    // Sort by count descending, then domain ascending
    items.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));

    println!("+--------------------------------+----------+");
    println!("| {:<30} | {:>8} |", "Domain", "Edits");
    println!("+--------------------------------+----------+");

    for (domain, count) in items.into_iter().take(3) {
        // Truncate domain if it's too long for the table
        let display_domain = if domain.len() > 30 {
            &domain[..27]
        } else {
            domain
        };
        
        println!("| {:<30} | {:>8} |", display_domain, count);
    }

    println!("+--------------------------------+----------+\n");
}

// Function to print the final leaderboard at the end of the run, showing top 10 domains
pub fn print_final_leaderboard(leaderboard: &Arc<Mutex<HashMap<String, u64>>>) {
    let Ok(map) = leaderboard.lock() else {
        return;
    };

    if map.is_empty() {
        println!("\n[FINAL LEADERBOARD] No data processed yet.\n");
        return;
    }

    let mut items: Vec<_> = map.iter().collect();
    // Sort by count descending, then by domain name ascending
    items.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));

    println!("\n========== [FINAL LEADERBOARD - TOP 10] ==========");
    println!("+------+--------------------------------+----------+");
    println!("| {:<4} | {:<30} | {:>8} |", "Rank", "Domain", "Edits");
    println!("+------+--------------------------------+----------+");

    for (index, (domain, count)) in items.into_iter().take(10).enumerate() {
        // Truncate long domain names to prevent breaking table borders
        let display_name = if domain.len() > 30 {
            format!("{}...", &domain[..27])
        } else {
            domain.to_string()
        };

        println!(
            "| {:<4} | {:<30} | {:>8} |",
            index + 1,
            display_name,
            count
        );
    }
    
    println!("+------+--------------------------------+----------+\n");
}

// Shared packet handler for both async and threaded binaries
pub fn handle_packet(
    raw_json: String,
    enqueue_time: Instant,
    leaderboard: &Arc<Mutex<HashMap<String, u64>>>,
    human_latencies: &Arc<Mutex<Vec<Duration>>>,
    bot_latencies: &Arc<Mutex<Vec<Duration>>>,
    human_jitters: &Arc<Mutex<Vec<Duration>>>,
    bot_jitters: &Arc<Mutex<Vec<Duration>>>,
    human_drifts: &Arc<Mutex<Vec<Duration>>>,
    bot_drifts: &Arc<Mutex<Vec<Duration>>>,
    run_stats: &Arc<Mutex<RunStats>>,
    degraded_mode: &AtomicBool,
    jitter_threshold_ms: u64,
    jitter_window_size: usize,
    total_packets: &std::sync::atomic::AtomicU64,
    total_violations: &std::sync::atomic::AtomicU64,
) {
    let dequeue_time = Instant::now();
    let drift = dequeue_time.duration_since(enqueue_time);
    let (violated, latency) = process_event(&raw_json, dequeue_time, leaderboard);
    total_packets.fetch_add(1, Ordering::SeqCst);

    // Record drift in the appropriate list
    if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(&raw_json) {
        let drift_list = if edit.bot { bot_drifts } else { human_drifts };
        if let Ok(mut list) = drift_list.lock() {
            list.push(drift);
        }
        let jitter = if edit.bot {
            record_jitter(bot_latencies, latency, jitter_window_size)
        } else {
            record_jitter(human_latencies, latency, jitter_window_size)
        };

        // Update degraded mode status based on jitter and record jitter values
        if let Some(jitter) = jitter {
            update_degraded_mode(jitter, jitter_threshold_ms, degraded_mode);
            let jitter_list = if edit.bot { bot_jitters } else { human_jitters };
            if let Ok(mut list) = jitter_list.lock() {
                list.push(jitter);
            }
        }
        if let Ok(mut stats) = run_stats.lock() {
            stats.record(drift, latency, violated, jitter);
        }

        // Log violations in red, normal packets in default color
        if violated {
            total_violations.fetch_add(1, Ordering::SeqCst);
            println!(
                "\x1b[31m[VIOLATION]\x1b[0m {} | Bot: {} | User: {} | Drift: {:?} | Latency: {:?}",
                edit.server, edit.bot, edit.user, drift, latency
            );
        } else {
            println!(
                "[OK] Server: {} | Bot: {} | User: {} | Drift: {:?} | Latency: {:?}",
                edit.server, edit.bot, edit.user, drift, latency
            );
        }
    }
}