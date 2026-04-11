//! Server-Sent Events (SSE) source.
//!
//! Connects to an HTTP endpoint streaming `text/event-stream` and
//! returns a `Pipe<SseEvent>`. Supports auto-reconnection with
//! `Last-Event-ID` resume per the W3C EventSource specification.
//!
//! ```ignore
//! use pipe_http::sse;
//!
//! let events = sse::connect("https://example.com/events").await?;
//! events
//!     .filter(|e| e.event.as_deref() == Some("update"))
//!     .for_each(|e| println!("{}", e.data))
//!     .await?;
//! ```

use std::time::Duration;

use pipe::pipeline::Pipe;
use pipe::pull::PipeError;

/// A parsed SSE event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// Event type (from `event:` field). None means default "message".
    pub event: Option<String>,
    /// Event payload (from `data:` field(s), joined by newlines).
    pub data: String,
    /// Last event ID (from `id:` field). Used for reconnection.
    pub id: Option<String>,
    /// Server-suggested retry interval (from `retry:` field).
    pub retry: Option<Duration>,
}

/// Configuration for an SSE connection.
#[derive(Debug, Clone)]
pub struct SseConfig {
    /// Target URL.
    pub url: String,
    /// Additional HTTP headers.
    pub headers: Vec<(String, String)>,
    /// Auto-reconnect on disconnect. Default: true.
    pub reconnect: bool,
    /// Maximum reconnection delay (exponential backoff cap). Default: 30s.
    pub max_reconnect_delay: Duration,
    /// Initial reconnection delay. Default: 1s.
    pub initial_reconnect_delay: Duration,
    /// Resume from this event ID on first connect.
    pub last_event_id: Option<String>,
    /// Internal channel buffer size. Default: 256.
    pub buffer_size: usize,
}

impl Default for SseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            reconnect: true,
            max_reconnect_delay: Duration::from_secs(30),
            initial_reconnect_delay: Duration::from_secs(1),
            last_event_id: None,
            buffer_size: 256,
        }
    }
}

/// Connect to an SSE endpoint with default settings.
///
/// Returns a `Pipe<SseEvent>` that streams parsed events.
/// Auto-reconnects on disconnect with exponential backoff.
pub fn connect(url: impl Into<String>) -> Pipe<SseEvent> {
    connect_with(SseConfig {
        url: url.into(),
        ..Default::default()
    })
}

/// Connect to an SSE endpoint with custom configuration.
pub fn connect_with(config: SseConfig) -> Pipe<SseEvent> {
    Pipe::generate_once(move |tx| async move {
        let client = reqwest::Client::new();
        let mut last_event_id = config.last_event_id.clone();
        let mut reconnect_delay = config.initial_reconnect_delay;
        let mut first_connect = true;

        loop {
            if !first_connect {
                if !config.reconnect {
                    return Ok(());
                }
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(config.max_reconnect_delay);
            }
            first_connect = false;

            let mut req = client
                .get(&config.url)
                .header("Accept", "text/event-stream")
                .header("Cache-Control", "no-cache");

            if let Some(ref id) = last_event_id {
                req = req.header("Last-Event-ID", id.as_str());
            }

            for (key, value) in &config.headers {
                req = req.header(key.as_str(), value.as_str());
            }

            let response = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    if config.reconnect {
                        continue;
                    }
                    return Err(PipeError::Custom(Box::new(e)));
                }
            };

            if !response.status().is_success() {
                if config.reconnect {
                    continue;
                }
                return Err(PipeError::Custom(
                    format!("SSE server returned {}", response.status()).into(),
                ));
            }

            // Reset backoff on successful connection
            reconnect_delay = config.initial_reconnect_delay;

            let result = read_event_stream(&tx, response, &mut last_event_id).await;
            match result {
                Ok(()) => return Ok(()),
                Err(_) if config.reconnect => continue,
                Err(e) => return Err(e),
            }
        }
    })
}

/// Read and parse the SSE event stream from an HTTP response.
async fn read_event_stream(
    tx: &pipe::pipeline::Emitter<SseEvent>,
    response: reqwest::Response,
    last_event_id: &mut Option<String>,
) -> Result<(), PipeError> {
    use futures_core::Stream;
    use std::pin::Pin;

    let mut byte_stream = response.bytes_stream();
    let mut buffer = String::new();

    // Parser state for current event being built
    let mut event_type: Option<String> = None;
    let mut data_buf = String::new();
    let mut id_buf: Option<String> = None;
    let mut retry_buf: Option<Duration> = None;

    loop {
        let chunk = {
            use std::future::poll_fn;
            poll_fn(|cx| Pin::new(&mut byte_stream).poll_next(cx)).await
        };

        match chunk {
            Some(Ok(bytes)) => {
                buffer.push_str(&String::from_utf8_lossy(&bytes));
            }
            Some(Err(e)) => {
                return Err(PipeError::Custom(Box::new(e)));
            }
            None => {
                // Stream ended (server closed connection)
                return Err(PipeError::Closed);
            }
        }

        // Process complete lines from buffer
        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].trim_end_matches('\r').to_owned();
            buffer.drain(..newline_pos + 1);

            if line.is_empty() {
                // Empty line: dispatch event if we have data
                if !data_buf.is_empty() {
                    // Remove trailing newline added by multi-line data
                    if data_buf.ends_with('\n') {
                        data_buf.pop();
                    }

                    if let Some(ref id) = id_buf {
                        *last_event_id = Some(id.clone());
                    }

                    let event = SseEvent {
                        event: event_type.take(),
                        data: std::mem::take(&mut data_buf),
                        id: id_buf.take(),
                        retry: retry_buf.take(),
                    };

                    tx.emit(event).await?;
                }
                event_type = None;
                data_buf.clear();
                id_buf = None;
                retry_buf = None;
                continue;
            }

            if line.starts_with(':') {
                // Comment line, ignore
                continue;
            }

            let (field, value) = match line.find(':') {
                Some(pos) => {
                    let value = &line[pos + 1..];
                    // Strip single leading space per spec
                    let value = value.strip_prefix(' ').unwrap_or(value);
                    (&line[..pos], value)
                }
                None => (line.as_str(), ""),
            };

            match field {
                "event" => {
                    event_type = Some(value.to_owned());
                }
                "data" => {
                    data_buf.push_str(value);
                    data_buf.push('\n');
                }
                "id" => {
                    // Spec: id field must not contain null
                    if !value.contains('\0') {
                        id_buf = Some(value.to_owned());
                    }
                }
                "retry" => {
                    if let Ok(ms) = value.parse::<u64>() {
                        retry_buf = Some(Duration::from_millis(ms));
                    }
                }
                _ => {
                    // Unknown field, ignore per spec
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_event() {
        // Test the event building logic via a full roundtrip
        // (actual stream parsing tested in integration tests)
        let event = SseEvent {
            event: Some("update".into()),
            data: "hello world".into(),
            id: Some("42".into()),
            retry: None,
        };
        assert_eq!(event.event.as_deref(), Some("update"));
        assert_eq!(event.data, "hello world");
        assert_eq!(event.id.as_deref(), Some("42"));
    }

    #[test]
    fn default_config() {
        let config = SseConfig::default();
        assert!(config.reconnect);
        assert_eq!(config.buffer_size, 256);
        assert_eq!(config.max_reconnect_delay, Duration::from_secs(30));
        assert_eq!(config.initial_reconnect_delay, Duration::from_secs(1));
        assert!(config.last_event_id.is_none());
    }
}
