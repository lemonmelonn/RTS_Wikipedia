// Async - Tokio

use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_util::io::StreamReader;
use tokio::io::{AsyncBufReadExt, BufReader};
use futures_util::StreamExt;
use std::fs::{create_dir_all, OpenOptions};
use std::time::Instant;
use std::io;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// Global counters to track performance
static TOTAL_PACKETS: AtomicU64 = AtomicU64::new(0);
static TOTAL_VIOLATIONS: AtomicU64 = AtomicU64::new(0);
static DEGRADED_MODE: AtomicBool = AtomicBool::new(false);

const JITTER_THRESHOLD_MS: u64 = 1;
const JITTER_WINDOW_SIZE: usize = 100;

// Import core logic from library
use rts_wiki::{handle_packet, print_final_leaderboard, print_final_statistics, print_top_three, Logger, RunStats, WikipediaEdit};

// Set run duration here
static MINUTES: u64 = 3;
const RUN_DURATION: Duration = Duration::from_secs(MINUTES * 60);

async fn start_ingestion(
    tx_human: mpsc::Sender<(String, Instant)>,
    tx_bot: mpsc::Sender<(String, Instant)>,
    logger: Logger,
) {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";
    
    // Wikipedia requires a User-Agent or it may drop the connection 
    let client = reqwest::Client::builder()
        .user_agent("RTS-Assignment-Async-Monitor")
        .build()
        .unwrap();
    
    loop {
        logger.logln("[ASYNC SENSOR] Connecting to Wikipedia...");
        let res = client.get(url).send().await;

        match res {
            Ok(response) => {
                logger.logln("Connected! Monitoring firehose...");
                
                let stream = response.bytes_stream().map(|result| {
                    result.map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                });
                let reader = StreamReader::new(stream);
                let mut lines = BufReader::new(reader).lines();

                // Async loop: non-blocking, can yield to other tasks
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
                            logger.elogln(&format!(
                                "[{:?}] OVERFLOW: Dropping packet",
                                Instant::now()
                            ));
                        }
                    }
                }
            }
            Err(e) => {
                logger.elogln(&format!("Connection failed: {}. Retrying...", e));
            }
        }
        
        // Network Resilience: Fail-Safe Mode [cite: 94, 95]
        logger.elogln("Stream ended or interrupted. Retrying in 2 seconds...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::main]
async fn main() {
    // Create bounded channels with a capacity of 100
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

    let _ = create_dir_all("logs");
    let log_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open("logs/asynclogs.txt")
        .unwrap();
    let logger = Logger::from_file(Arc::new(Mutex::new(log_file)));

    let logger_ingest = logger.clone();
    tokio::spawn(async move {
        start_ingestion(tx_human, tx_bot, logger_ingest).await;
    });

    // Spawn the Leaderboard Printer Task
    let leaderboard_printer = Arc::clone(&leaderboard);
    let print_leaderboard_flag = Arc::clone(&print_leaderboard);
    let logger_printer = logger.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if !print_leaderboard_flag.load(Ordering::SeqCst) {
                break;
            }
            print_top_three(&leaderboard_printer, &logger_printer);
        }
    });

    logger.logln("Async System Active. 10s Watchdog engaged. Monitoring for 2ms deadlines...");

    loop {
        // Check for run duration
        if start_time.elapsed() >= RUN_DURATION {
            break;
        }
        // Requirement: Watchdog Timer
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
                &logger,
                &DEGRADED_MODE,
                JITTER_THRESHOLD_MS,
                JITTER_WINDOW_SIZE,
                &TOTAL_PACKETS,
                &TOTAL_VIOLATIONS,
            );
            continue;
        }

        // [WATCHDOG TIMER] Wait for next packet with a 10s timeout
        // If no packets arrive, trigger a reset (print message and continue waiting).
        let next_packet = timeout(
            Duration::from_secs(10),
            async {
                if DEGRADED_MODE.load(Ordering::SeqCst) {
                    rx_human.recv().await
                } else {
                    // Prioritize human packets
                    tokio::select! {
                        biased;
                        packet = rx_human.recv() => packet,
                        packet = rx_bot.recv() => packet,
                    }
                }
            },
        )
        .await;

        // Handle the result of the timeout
        match next_packet {
            Ok(Some((raw_json, enqueue_time))) => {
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
                    &logger,
                    &DEGRADED_MODE,
                    JITTER_THRESHOLD_MS,
                    JITTER_WINDOW_SIZE,
                    &TOTAL_PACKETS,
                    &TOTAL_VIOLATIONS,
                );
            }
            Ok(None) => break,
            Err(_) => {
                logger.logln(&format!(
                    "[{:?}] WATCHDOG: No data for 10s. Triggering Reset...",
                    Instant::now()
                ));
            }
        }
    }

    // Signal the full leaderboard printer to stop and print final results
    print_leaderboard.store(false, Ordering::SeqCst);
    logger.logln(&format!(
        "\n============= [RUN COMPLETE] Duration: {:?}============\n",
        RUN_DURATION
    ));
    print_final_leaderboard(&leaderboard, &logger);

    if let Ok(stats) = run_stats.lock() {
        print_final_statistics(
            &stats,
            &human_latencies,
            &bot_latencies,
            &human_jitters,
            &bot_jitters,
            &human_drifts,
            &bot_drifts,
            &logger,
        );
    }
}


