// Async - Tokio

use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_util::io::StreamReader;
use tokio::io::{AsyncBufReadExt, BufReader};
use futures_util::StreamExt;
use std::time::Instant;
use std::io;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
//use std::sync::Arc;

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

async fn start_ingestion(
    tx_human: mpsc::Sender<(String, Instant)>,
    tx_bot: mpsc::Sender<(String, Instant)>,
) {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";
    
    // Wikipedia requires a User-Agent or it may drop the connection 
    let client = reqwest::Client::builder()
        .user_agent("RTS-Assignment-Student-Monitor/1.0 (Contact: student@apu.edu.my)")
        .build()
        .unwrap();
    
    loop {
        println!("[ASYNC SENSOR] Connecting to Wikipedia...");
        let res = client.get(url).send().await;

        match res {
            Ok(response) => {
                println!("Connected! Monitoring firehose...");
                
                let stream = response.bytes_stream().map(|result| {
                    result.map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                });
                let reader = StreamReader::new(stream);
                let mut lines = BufReader::new(reader).lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    if line.starts_with("data: ") {
                        // 1. CAPTURE ARRIVAL TIME IMMEDIATELY
                        let enqueue_time = Instant::now(); 
                        
                        let json_str = &line[6..]; 
                        
                        // 2. ROUTE TO HUMAN/BOT CHANNEL (PRIORITIZE HUMAN ON CONSUME)
                        let is_bot = serde_json::from_str::<WikipediaEdit>(json_str)
                            .map(|edit| edit.bot)
                            .unwrap_or(false);

                        let send_result = if is_bot {
                            tx_bot.try_send((json_str.to_string(), enqueue_time))
                        } else {
                            tx_human.try_send((json_str.to_string(), enqueue_time))
                        };

                        if let Err(_) = send_result {
                            eprintln!("[{:?}] OVERFLOW: Dropping packet", Instant::now());
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Connection failed: {}. Retrying...", e);
            }
        }
        
        // Network Resilience: Fail-Safe Mode [cite: 94, 95]
        eprintln!("Stream ended or interrupted. Retrying in 2 seconds...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::main]
async fn main() {
    let (tx_human, mut rx_human) = mpsc::channel::<(String, Instant)>(100);
    let (tx_bot, mut rx_bot) = mpsc::channel::<(String, Instant)>(100);
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

    tokio::spawn(async move {
        start_ingestion(tx_human, tx_bot).await;
    });

    let leaderboard_printer = Arc::clone(&leaderboard);
    let print_leaderboard_flag = Arc::clone(&print_leaderboard);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            if !print_leaderboard_flag.load(Ordering::SeqCst) {
                break;
            }
            print_top_three(&leaderboard_printer);
        }
    });

    println!("System Active. 10s Watchdog engaged.");

    loop {
        if start_time.elapsed() >= RUN_DURATION {
            break;
        }
        // Requirement: Watchdog Timer (Source 94)
        // Priority: humans first. Only read bot channel when human channel is empty.
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
            continue;
        }

        let next_packet = timeout(
            Duration::from_secs(10),
            async {
                if DEGRADED_MODE.load(Ordering::SeqCst) {
                    rx_human.recv().await
                } else {
                    tokio::select! {
                        biased;
                        packet = rx_human.recv() => packet,
                        packet = rx_bot.recv() => packet,
                    }
                }
            },
        )
        .await;

        match next_packet {
            Ok(Some((raw_json, enqueue_time))) => handle_packet(
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
            ),
            Ok(None) => break,
            Err(_) => {
                println!("[{:?}] WATCHDOG: No data for 10s. Triggering Reset...", Instant::now());
            }
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
                edit.server,
                edit.bot,
                edit.user,
                drift,
                latency
            );
        }
    }
}


