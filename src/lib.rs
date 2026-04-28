// src/lib.rs
use serde::Deserialize;
use std::time::{Instant, Duration};

#[derive(Deserialize, Debug)]
pub struct WikipediaEdit<'a> {
    #[serde(rename = "server_name")]
    pub server: &'a str, 
    pub user: &'a str,
    pub bot: bool,
}

// Move the core analysis here so Criterion can call it
pub fn process_event(raw_json: &str, arrival_time: Instant) -> (bool, Duration) {
    if let Ok(edit) = serde_json::from_str::<WikipediaEdit>(raw_json) {
        let latency = arrival_time.elapsed();
        let violated = latency > Duration::from_millis(2);
        return (violated, latency);
    }
    (false, Duration::from_secs(0))
}