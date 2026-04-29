// Multithreaded

use std::sync::mpsc;
use std::time::{Duration, Instant};
use std::thread;
use std::io::{BufRead, BufReader};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// Global counters to track performance
static TOTAL_PACKETS: AtomicU64 = AtomicU64::new(0);
static TOTAL_VIOLATIONS: AtomicU64 = AtomicU64::new(0);
static DEGRADED_MODE: AtomicBool = AtomicBool::new(false);

const JITTER_THRESHOLD_MS: u64 = 2;
const JITTER_WINDOW_SIZE: usize = 100;

// Replace 'wikipedia_monitor' with the name in your Cargo.toml
use rts_wiki::{print_final_leaderboard, print_final_statistics, print_top_three, process_event, record_jitter, update_degraded_mode, RunStats, WikipediaEdit};

// Set run duration here
static MINUTES: u64 = 1;
const RUN_DURATION: Duration = Duration::from_secs(MINUTES * 10);

fn start_blocking_ingestion(
    tx_human: mpsc::SyncSender<(String, Instant)>,
    tx_bot: mpsc::SyncSender<(String, Instant)>,
) {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";
    
    let client = reqwest::blocking::Client::builder()
        .user_agent("RTS-Assignment-Threaded-Monitor/1.0")
        .build()
        .unwrap();
    
    loop {
        println!("[THREADED SENSOR] Connecting...");
        let res = client.get(url).send();

        match res {
            Ok(response) => {
                let mut reader = BufReader::new(response);
                let mut line = String::new();

                // Blocking loop: stays on this thread
                while let Ok(len) = reader.read_line(&mut line) {
                    if len == 0 { break; }
                    
                    if line.starts_with("data: ") {
                        let enqueue_time = Instant::now();
                        let json_str = line[6..].trim().to_string();

                        let is_bot = serde_json::from_str::<WikipediaEdit>(&json_str)
                            .map(|edit| edit.bot)
                            .unwrap_or(false);

                        let send_result = if is_bot {
                            tx_bot.try_send((json_str, enqueue_time))
                        } else {
                            tx_human.try_send((json_str, enqueue_time))
                        };

                        // try_send handles backpressure without blocking the sensor
                        if let Err(_) = send_result {
                            eprintln!("[OVERFLOW] Threaded queue full, dropping packet");
                        }
                    }
                    line.clear();
                }
            }
            Err(e) => eprintln!("Connection failed: {}. Retrying...", e),
        }
        thread::sleep(Duration::from_secs(2));
    }
}

fn main() {
    // 1. Bounded Synchronous Channels (Capacity of 100 each)
    let (tx_human, rx_human) = mpsc::sync_channel::<(String, Instant)>(100);
    let (tx_bot, rx_bot) = mpsc::sync_channel::<(String, Instant)>(100);
    let leaderboard = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let print_leaderboard = Arc::new(AtomicBool::new(true));
    let human_latencies = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let bot_latencies = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let human_jitters = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let bot_jitters = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let human_drifts = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let bot_drifts = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let run_stats = Arc::new(Mutex::new(RunStats::default()));
    let start_time = Instant::now();

    // 2. Spawn the Ingestion Thread
    thread::spawn(move || {
        start_blocking_ingestion(tx_human, tx_bot);
    });

    let leaderboard_printer = Arc::clone(&leaderboard);
    let print_leaderboard_flag = Arc::clone(&print_leaderboard);
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(3));
        if !print_leaderboard_flag.load(Ordering::SeqCst) {
            break;
        }
        print_top_three(&leaderboard_printer);
    });

    println!("Threaded System Active. Monitoring for 2ms deadlines...");

    let mut last_received = Instant::now();
    loop {
        if start_time.elapsed() >= RUN_DURATION {
            break;
        }
        if let Ok((raw_json, enqueue_time)) = rx_human.try_recv() {
            handle_packet(
                raw_json,
                enqueue_time,
                &leaderboard,
                &human_latencies,
                &bot_latencies,
                &human_jitters,
                &bot_jitters,
                &human_drifts,
                &bot_drifts,
                &run_stats,
            );
            last_received = Instant::now();
            continue;
        }

        if !DEGRADED_MODE.load(Ordering::SeqCst) {
            if let Ok((raw_json, enqueue_time)) = rx_bot.try_recv() {
                handle_packet(
                    raw_json,
                    enqueue_time,
                    &leaderboard,
                    &human_latencies,
                    &bot_latencies,
                    &human_jitters,
                    &bot_jitters,
                    &human_drifts,
                    &bot_drifts,
                    &run_stats,
                );
                last_received = Instant::now();
                continue;
            }
        }

        // 3. Watchdog Logic using recv_timeout
        // This blocks the main thread for max 100ms waiting for human data
        match rx_human.recv_timeout(Duration::from_millis(100)) {
            Ok((raw_json, enqueue_time)) => {
                handle_packet(
                    raw_json,
                    enqueue_time,
                    &leaderboard,
                    &human_latencies,
                    &bot_latencies,
                    &human_jitters,
                    &bot_jitters,
                    &human_drifts,
                    &bot_drifts,
                    &run_stats,
                );
                last_received = Instant::now();
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if last_received.elapsed() >= Duration::from_secs(10) {
                    println!("\n\n[WATCHDOG] No data received for 10s. Check connection.\n\n");
                    last_received = Instant::now();
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    print_leaderboard.store(false, Ordering::SeqCst);
    println!("\n============= [RUN COMPLETE] Duration: {:?}============\n", RUN_DURATION);
    print_final_leaderboard(&leaderboard);

    if let Ok(stats) = run_stats.lock() {
        print_final_statistics(
            &stats,
            &human_latencies,
            &bot_latencies,
            &human_jitters,
            &bot_jitters,
            &human_drifts,
            &bot_drifts,
        );
    }
}

fn handle_packet(
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
) {
    let dequeue_time = Instant::now();
    let drift = dequeue_time.duration_since(enqueue_time);
    let (violated, latency) = process_event(&raw_json, dequeue_time, leaderboard);
    TOTAL_PACKETS.fetch_add(1, Ordering::SeqCst);

    if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(&raw_json) {
        let drift_list = if edit.bot { bot_drifts } else { human_drifts };
        if let Ok(mut list) = drift_list.lock() {
            list.push(drift);
        }
        let jitter = if edit.bot {
            record_jitter(bot_latencies, latency, JITTER_WINDOW_SIZE)
        } else {
            record_jitter(human_latencies, latency, JITTER_WINDOW_SIZE)
        };

        if let Some(jitter) = jitter {
            update_degraded_mode(jitter, JITTER_THRESHOLD_MS, &DEGRADED_MODE);
            let jitter_list = if edit.bot { bot_jitters } else { human_jitters };
            if let Ok(mut list) = jitter_list.lock() {
                list.push(jitter);
            }
        }
        if let Ok(mut stats) = run_stats.lock() {
            stats.record(drift, latency, violated, jitter);
        }
        if violated {
            TOTAL_VIOLATIONS.fetch_add(1, Ordering::SeqCst);
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
