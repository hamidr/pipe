//! Server-Sent Events (SSE) source.
//!
//! Connects to an HTTP endpoint streaming `text/event-stream` and
//! returns a `Pipe<SseEvent>`. Supports auto-reconnection with
//! `Last-Event-ID` resume per the W3C EventSource specification.
//!
//! The returned pipe is single-use -- cloning and materializing both
//! clones returns an error (same as `Pipe::generate_once`).
//!
//! ```ignore
//! use lazyflow_http::sse;
//!
//! sse::connect("https://example.com/events")
//!     .filter(|e| e.event.as_deref() == Some("update"))
//!     .for_each(|e| println!("{}", e.data))
//!     .await?;
//! ```

use std::time::Duration;

use lazyflow::pipeline::Pipe;
use lazyflow::pull::PipeError;

/// Maximum bytes to buffer before seeing a newline. Prevents OOM from
/// a malicious server sending data without line breaks.
const MAX_LINE_BUFFER: usize = 16 * 1024 * 1024;

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
    /// Auto-reconnect on disconnect or 5xx errors. Default: true.
    /// Does not reconnect on 4xx client errors.
    pub reconnect: bool,
    /// Maximum reconnection delay (exponential backoff cap). Default: 30s.
    pub max_reconnect_delay: Duration,
    /// Initial reconnection delay. Default: 1s.
    pub initial_reconnect_delay: Duration,
    /// Resume from this event ID on first connect.
    pub last_event_id: Option<String>,
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
        }
    }
}

/// Connect to an SSE endpoint with default settings.
///
/// Returns a `Pipe<SseEvent>` that streams parsed events.
/// Auto-reconnects on disconnect or 5xx errors with exponential backoff.
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

            let status = response.status();
            if !status.is_success() {
                // 4xx: client error, reconnecting won't help
                if status.is_client_error() {
                    return Err(PipeError::Custom(
                        format!("SSE server returned {status}").into(),
                    ));
                }
                // 5xx: server error, retry if configured
                if config.reconnect {
                    continue;
                }
                return Err(PipeError::Custom(
                    format!("SSE server returned {status}").into(),
                ));
            }

            // Reset backoff on successful connection
            reconnect_delay = config.initial_reconnect_delay;

            let result = read_event_stream(
                &tx,
                response,
                &mut last_event_id,
                &mut reconnect_delay,
                &config,
            )
            .await;
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
    tx: &lazyflow::pipeline::Emitter<SseEvent>,
    response: reqwest::Response,
    last_event_id: &mut Option<String>,
    reconnect_delay: &mut Duration,
    config: &SseConfig,
) -> Result<(), PipeError> {
    use futures_core::Stream;
    use std::pin::Pin;

    let mut byte_stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut strip_bom = true;

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
                let text = std::str::from_utf8(&bytes).map_err(|e| {
                    PipeError::Custom(format!("SSE stream contains invalid UTF-8: {e}").into())
                })?;
                buffer.push_str(text);
            }
            Some(Err(e)) => {
                return Err(PipeError::Custom(Box::new(e)));
            }
            None => {
                return Err(PipeError::Closed);
            }
        }

        // Strip leading UTF-8 BOM on first chunk per W3C spec
        if strip_bom {
            if buffer.starts_with('\u{FEFF}') {
                buffer.drain(..3);
            }
            strip_bom = false;
        }

        // Guard against unbounded buffer growth from missing newlines
        if buffer.len() > MAX_LINE_BUFFER && !buffer.contains('\n') {
            return Err(PipeError::Custom(
                format!(
                    "SSE line buffer exceeded {} bytes without newline",
                    MAX_LINE_BUFFER,
                )
                .into(),
            ));
        }

        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].trim_end_matches('\r').to_owned();
            buffer.drain(..newline_pos + 1);

            if line.is_empty() {
                if !data_buf.is_empty() {
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
                continue;
            }

            let (field, value) = match line.find(':') {
                Some(pos) => {
                    let value = &line[pos + 1..];
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
                    if !value.contains('\0') {
                        id_buf = Some(value.to_owned());
                    }
                }
                "retry" => {
                    if let Ok(ms) = value.parse::<u64>() {
                        let duration = Duration::from_millis(ms);
                        retry_buf = Some(duration);
                        // Apply server-suggested retry interval
                        *reconnect_delay = duration.min(config.max_reconnect_delay);
                    }
                }
                _ => {}
            }
        }
    }
}

// -- Parser logic extracted for unit testing --

#[cfg(test)]
fn parse_field(line: &str) -> Option<(&str, &str)> {
    if line.is_empty() || line.starts_with(':') {
        return None;
    }
    match line.find(':') {
        Some(pos) => {
            let value = &line[pos + 1..];
            let value = value.strip_prefix(' ').unwrap_or(value);
            Some((&line[..pos], value))
        }
        None => Some((line, "")),
    }
}

#[cfg(test)]
fn parse_events(text: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut event_type: Option<String> = None;
    let mut data_buf = String::new();
    let mut id_buf: Option<String> = None;
    let mut retry_buf: Option<Duration> = None;

    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r');

        if line.is_empty() {
            if !data_buf.is_empty() {
                if data_buf.ends_with('\n') {
                    data_buf.pop();
                }
                events.push(SseEvent {
                    event: event_type.take(),
                    data: std::mem::take(&mut data_buf),
                    id: id_buf.take(),
                    retry: retry_buf.take(),
                });
            }
            event_type = None;
            data_buf.clear();
            id_buf = None;
            retry_buf = None;
            continue;
        }

        if let Some((field, value)) = parse_field(line) {
            match field {
                "event" => event_type = Some(value.to_owned()),
                "data" => {
                    data_buf.push_str(value);
                    data_buf.push('\n');
                }
                "id" => {
                    if !value.contains('\0') {
                        id_buf = Some(value.to_owned());
                    }
                }
                "retry" => {
                    if let Ok(ms) = value.parse::<u64>() {
                        retry_buf = Some(Duration::from_millis(ms));
                    }
                }
                _ => {}
            }
        }
    }

    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_field_data() {
        assert_eq!(parse_field("data: hello"), Some(("data", "hello")));
    }

    #[test]
    fn parse_field_strips_single_leading_space() {
        assert_eq!(
            parse_field("data:  two spaces"),
            Some(("data", " two spaces"))
        );
    }

    #[test]
    fn parse_field_no_value() {
        assert_eq!(parse_field("data"), Some(("data", "")));
    }

    #[test]
    fn parse_field_empty_value() {
        assert_eq!(parse_field("data:"), Some(("data", "")));
    }

    #[test]
    fn parse_field_comment() {
        assert_eq!(parse_field(": this is a comment"), None);
    }

    #[test]
    fn parse_field_empty_line() {
        assert_eq!(parse_field(""), None);
    }

    #[test]
    fn parse_events_single() {
        let text = "event: update\ndata: hello\nid: 1\n\n";
        let events = parse_events(text);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.as_deref(), Some("update"));
        assert_eq!(events[0].data, "hello");
        assert_eq!(events[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn parse_events_multiline_data() {
        let text = "data: line1\ndata: line2\ndata: line3\n\n";
        let events = parse_events(text);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line1\nline2\nline3");
    }

    #[test]
    fn parse_events_multiple() {
        let text = "data: first\n\ndata: second\n\n";
        let events = parse_events(text);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data, "first");
        assert_eq!(events[1].data, "second");
    }

    #[test]
    fn parse_events_comments_ignored() {
        let text = ": comment\ndata: hello\n\n";
        let events = parse_events(text);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn parse_events_retry_field() {
        let text = "data: hello\nretry: 5000\n\n";
        let events = parse_events(text);
        assert_eq!(events[0].retry, Some(Duration::from_millis(5000)));
    }

    #[test]
    fn parse_events_id_with_null_rejected() {
        let text = "data: hello\nid: bad\0id\n\n";
        let events = parse_events(text);
        assert_eq!(events[0].id, None);
    }

    #[test]
    fn parse_events_no_data_no_dispatch() {
        let text = "event: ping\n\n";
        let events = parse_events(text);
        assert!(events.is_empty());
    }

    #[test]
    fn parse_events_unknown_fields_ignored() {
        let text = "data: hello\nfoo: bar\n\n";
        let events = parse_events(text);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn parse_events_field_without_colon() {
        let text = "data\n\n";
        let events = parse_events(text);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "");
    }

    #[test]
    fn default_config() {
        let config = SseConfig::default();
        assert!(config.reconnect);
        assert_eq!(config.max_reconnect_delay, Duration::from_secs(30));
        assert_eq!(config.initial_reconnect_delay, Duration::from_secs(1));
        assert!(config.last_event_id.is_none());
    }
}
