//! SSE consumer example.
//!
//! Connects to an SSE endpoint, filters for specific event types, and
//! prints them. Demonstrates lazyflow-http SSE source with lazyflow operators.
//!
//! Usage:
//!   cargo run -p lazyflow-http --example sse_consumer -- <url> [event-type]
//!
//! Examples:
//!   cargo run -p lazyflow-http --example sse_consumer -- https://stream.wikimedia.org/v2/stream/recentchange
//!   cargo run -p lazyflow-http --example sse_consumer -- https://stream.wikimedia.org/v2/stream/recentchange message

use std::time::Duration;

use lazyflow_http::sse::{self, SseConfig};

#[tokio::main]
async fn main() -> Result<(), lazyflow::pull::PipeError> {
    let args: Vec<String> = std::env::args().collect();
    let url = args.get(1).expect("Usage: sse_consumer <url> [event-type]");
    let event_filter = args.get(2).cloned();

    println!("connecting to {url}");
    if let Some(ref f) = event_filter {
        println!("filtering for event type: {f}");
    }
    println!("---");

    let config = SseConfig {
        url: url.clone(),
        reconnect: true,
        initial_reconnect_delay: Duration::from_secs(1),
        max_reconnect_delay: Duration::from_secs(10),
        ..Default::default()
    };

    let pipe = sse::connect_with(config);

    let pipe = if let Some(filter) = event_filter {
        pipe.filter(move |e| e.event.as_deref() == Some(filter.as_str()))
    } else {
        pipe
    };

    pipe.for_each(|event| {
        let event_type = event.event.as_deref().unwrap_or("(default)");
        let id = event.id.as_deref().unwrap_or("(none)");
        let preview = if event.data.len() > 120 {
            format!("{}...", &event.data[..120])
        } else {
            event.data.clone()
        };
        println!("[{event_type}] id={id} {preview}");
    })
    .await?;

    Ok(())
}
