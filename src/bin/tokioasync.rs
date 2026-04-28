// Async - Tokio

use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_util::io::StreamReader;
use tokio::io::{AsyncBufReadExt, BufReader};
use futures_util::StreamExt;
use std::time::Instant;
use std::io;

use std::sync::atomic::{AtomicU64, Ordering};
//use std::sync::Arc;

// Global counters to track performance
static TOTAL_PACKETS: AtomicU64 = AtomicU64::new(0);
static TOTAL_VIOLATIONS: AtomicU64 = AtomicU64::new(0);

// Replace 'wikipedia_monitor' with the name in your Cargo.toml
use rts_wiki::{WikipediaEdit, process_event};


async fn start_ingestion(tx: mpsc::Sender<(String, Instant)>) {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";
    
    // Wikipedia requires a User-Agent or it may drop the connection 
    let client = reqwest::Client::builder()
        .user_agent("RTS-Assignment-Student-Monitor/1.0 (Contact: student@apu.edu.my)")
        .build()
        .unwrap();
    
    loop {
        println!("Connecting to Wikipedia...");
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
                        let arrival_time = Instant::now(); 
                        
                        let json_str = &line[6..]; 
                        
                        // 2. SEND AS A TUPLE
                        // Note: we send the string and the timestamp together
                        if let Err(_) = tx.try_send((json_str.to_string(), arrival_time)) {
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
    // let (tx, mut rx) = mpsc::channel::<String>(100);
    let (tx, mut rx) = mpsc::channel::<(String, Instant)>(100);

    tokio::spawn(async move {
        start_ingestion(tx).await;
    });

    println!("System Active. 10s Watchdog engaged.");

    loop {
        // Requirement: Watchdog Timer (Source 94)
        match timeout(Duration::from_secs(10), rx.recv()).await {
            Ok(Some((raw_json, arrival_time))) => {
                // Zero-Copy Parsing
                let (violated, latency) = process_event(&raw_json, arrival_time);
                TOTAL_PACKETS.fetch_add(1, Ordering::SeqCst);

                // Re-parse just for the print statement name (or modify lib to return it)
                if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(&raw_json) {
                    if violated {
                        TOTAL_VIOLATIONS.fetch_add(1, Ordering::SeqCst);
                        println!("\x1b[31m[VIOLATION]\x1b[0m {} took {:?}", edit.server, latency);
                    } else {
                        println!("[OK] Server: {} | Latency: {:?} | Total Packets: {}", edit.server, latency, TOTAL_PACKETS.load(Ordering::SeqCst));
                    }
                }

                // // Re-parse just for the print statement name (or modify lib to return it)
                // if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(&raw_json) {
                    
                //     // 3. CALCULATE TOTAL LATENCY (Queue Time + Parse Time)
                //     let total_latency = arrival_time.elapsed();
                    
                //     if total_latency > Duration::from_millis(2) {
                //         println!("\x1b[31m[VIOLATION]\x1b[0m {} took {:?}", edit.server, total_latency);
                //     } else {
                //         println!("[OK] Server: {} | User: {} | Latency: {:?}", edit.server, edit.user, total_latency);
                //     }
                // }
            }
            Ok(None) => break, 
            Err(_) => {
                println!("[{:?}] WATCHDOG: No data for 10s. Triggering Reset...", Instant::now());
            }
        }
    }
}

