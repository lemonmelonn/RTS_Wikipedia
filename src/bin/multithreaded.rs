use std::sync::mpsc;
use std::time::{Duration, Instant};
use std::thread;
use std::io::{BufRead, BufReader};

use std::sync::atomic::{AtomicU64, Ordering};

// Global counters to track performance
static TOTAL_PACKETS: AtomicU64 = AtomicU64::new(0);
static TOTAL_VIOLATIONS: AtomicU64 = AtomicU64::new(0);

// Replace 'wikipedia_monitor' with the name in your Cargo.toml
use rts_wiki::{WikipediaEdit, process_event};

fn start_blocking_ingestion(tx: mpsc::SyncSender<(String, Instant)>) {
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
                        let arrival_time = Instant::now();
                        let json_str = line[6..].trim().to_string();

                        // try_send handles backpressure without blocking the sensor
                        if let Err(_) = tx.try_send((json_str, arrival_time)) {
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
    // 1. Bounded Synchronous Channel (Capacity of 100)
    let (tx, rx) = mpsc::sync_channel::<(String, Instant)>(100);

    // 2. Spawn the Ingestion Thread
    thread::spawn(move || {
        start_blocking_ingestion(tx);
    });

    println!("Threaded System Active. Monitoring for 2ms deadlines...");

    loop {
        // 3. Watchdog Logic using recv_timeout
        // This blocks the main thread for max 10s waiting for data
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok((raw_json, arrival_time)) => {
                // CALL THE SHARED FUNCTION INSTEAD OF WRITING THE LOGIC HERE
                let (violated, latency) = process_event(&raw_json, arrival_time);
                
                TOTAL_PACKETS.fetch_add(1, Ordering::SeqCst);

                // Re-parse just for the print statement name (or modify lib to return it)
                if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(&raw_json) {
                    if violated {
                        TOTAL_VIOLATIONS.fetch_add(1, Ordering::SeqCst);
                        println!("\x1b[31m[VIOLATION]\x1b[0m {} took {:?}", edit.server, latency);
                    } else {
                        println!("[OK] Server: {} | Latency: {:?}", edit.server, latency);
                    }
                }

                // // Zero-copy parsing
                // if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(&raw_json) {
                //     let total_latency = arrival_time.elapsed();
                    
                //     if total_latency > Duration::from_millis(2) {
                //         println!("\x1b[31m[VIOLATION]\x1b[0m {} took {:?}", edit.server, total_latency);
                //     } else {
                //         println!("[OK] Server: {} | Latency: {:?}", edit.server, total_latency);
                //     }
                // }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                println!("[WATCHDOG] No data received for 10s. Check connection.");
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}