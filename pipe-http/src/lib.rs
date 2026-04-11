//! HTTP connectors for `pipe` -- SSE sources and HTTP sinks.
//!
//! ```ignore
//! use pipe_http::sse;
//!
//! let events = sse::connect("https://example.com/events");
//! events
//!     .filter(|e| e.event.as_deref() == Some("update"))
//!     .map(|e| e.data)
//!     .for_each(|data| println!("{data}"))
//!     .await?;
//! ```

pub mod sse;
pub mod ws;
