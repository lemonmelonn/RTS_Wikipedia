// src/lib.rs
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Deserialize, Debug)]
pub struct WikipediaEdit<'a> {
    #[serde(rename = "server_name")]
    pub server: &'a str, // zero-copy 
    pub user: &'a str, // zero-copy
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

fn update_leaderboard(leaderboard: &Arc<Mutex<HashMap<String, u64>>>, domain: &str) {
    if let Ok(mut map) = leaderboard.lock() {
        *map.entry(domain.to_string()).or_insert(0) += 1;
    }
}

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

    let mut min = window[0];
    let mut max = window[0];
    for value in window.iter().skip(1) {
        if *value < min {
            min = *value;
        }
        if *value > max {
            max = *value;
        }
    }

    Some(max - min)
}

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

pub fn print_top_three(leaderboard: &Arc<Mutex<HashMap<String, u64>>>) {
    let Ok(map) = leaderboard.lock() else {
        return;
    };

    if map.is_empty() {
        println!("[LEADERBOARD] No data yet.");
        return;
    }

    let mut items: Vec<_> = map.iter().collect();
    items.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));

    let top = items
        .into_iter()
        .take(3)
        .map(|(domain, count)| format!("{} ({})", domain, count))
        .collect::<Vec<_>>()
        .join(" | ");

    println!("\n\n[LEADERBOARD] Top 3: {}\n\n", top);
}

