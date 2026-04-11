//! WebSocket source and sink.
//!
//! Connects to a WebSocket endpoint and returns a `Pipe<WsMessage>` for
//! incoming messages plus a cloneable [`WsSender`] for outgoing messages.
//!
//! ```ignore
//! use pipe_http::ws;
//!
//! let (incoming, sender) = ws::connect("wss://example.com/ws").await?;
//!
//! // Send messages
//! sender.send_text("hello").await?;
//!
//! // Process incoming as a pipe
//! incoming
//!     .filter(|m| m.is_text())
//!     .map(|m| m.into_text())
//!     .for_each(|text| println!("{text}"))
//!     .await?;
//! ```

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use pipe::pipeline::Pipe;
use pipe::pull::PipeError;
use tokio::sync::Mutex;

/// A WebSocket message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsMessage {
    Text(String),
    Binary(Vec<u8>),
    Close,
}

impl WsMessage {
    pub fn is_text(&self) -> bool {
        matches!(self, WsMessage::Text(_))
    }

    pub fn is_binary(&self) -> bool {
        matches!(self, WsMessage::Binary(_))
    }

    pub fn is_close(&self) -> bool {
        matches!(self, WsMessage::Close)
    }

    /// Extract text payload. Returns empty string for non-text messages.
    pub fn into_text(self) -> String {
        match self {
            WsMessage::Text(s) => s,
            _ => String::new(),
        }
    }

    /// Extract binary payload. Returns empty vec for non-binary messages.
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            WsMessage::Binary(b) => b,
            WsMessage::Text(s) => s.into_bytes(),
            WsMessage::Close => Vec::new(),
        }
    }
}

/// Cloneable handle for sending WebSocket messages.
///
/// Each `send_*` call is mutex-protected. For multi-part messages,
/// build the full payload first and send in one call.
#[derive(Clone)]
pub struct WsSender {
    tx: Arc<Mutex<futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::Message,
    >>>,
}

impl WsSender {
    /// Send a text message.
    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), PipeError> {
        let msg = tokio_tungstenite::tungstenite::Message::text(text.into());
        self.tx
            .lock()
            .await
            .send(msg)
            .await
            .map_err(|e| PipeError::Custom(Box::new(e)))
    }

    /// Send a binary message.
    pub async fn send_binary(&self, data: Vec<u8>) -> Result<(), PipeError> {
        let msg = tokio_tungstenite::tungstenite::Message::binary(data);
        self.tx
            .lock()
            .await
            .send(msg)
            .await
            .map_err(|e| PipeError::Custom(Box::new(e)))
    }

    /// Send a close frame, initiating graceful shutdown.
    pub async fn close(&self) -> Result<(), PipeError> {
        self.tx
            .lock()
            .await
            .send(tokio_tungstenite::tungstenite::Message::Close(None))
            .await
            .map_err(|e| PipeError::Custom(Box::new(e)))
    }
}

/// Connect to a WebSocket endpoint.
///
/// Returns a `(Pipe<WsMessage>, WsSender)` pair. The pipe streams
/// incoming messages; the sender allows sending outgoing messages.
/// Ping/pong frames are handled automatically and not exposed.
///
/// The pipe is single-use (same as `Pipe::generate_once`). The sender
/// remains usable as long as the connection is open.
///
/// The connection closes when the pipe is dropped or the server sends
/// a close frame.
pub async fn connect(url: impl Into<String>) -> Result<(Pipe<WsMessage>, WsSender), PipeError> {
    let url = url.into();
    let (stream, _response) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| PipeError::Custom(Box::new(e)))?;

    let (write_half, read_half) = stream.split();
    let sender = WsSender {
        tx: Arc::new(Mutex::new(write_half)),
    };

    let sender_for_pong = sender.tx.clone();
    let incoming = Pipe::generate_once(move |tx| async move {
        let mut read = read_half;
        while let Some(result) = read.next().await {
            match result {
                Ok(msg) => {
                    let ws_msg = match msg {
                        tokio_tungstenite::tungstenite::Message::Text(s) => {
                            WsMessage::Text(s.to_string())
                        }
                        tokio_tungstenite::tungstenite::Message::Binary(b) => {
                            WsMessage::Binary(b.to_vec())
                        }
                        tokio_tungstenite::tungstenite::Message::Ping(data) => {
                            let pong = tokio_tungstenite::tungstenite::Message::Pong(data);
                            let _ = sender_for_pong.lock().await.send(pong).await;
                            continue;
                        }
                        tokio_tungstenite::tungstenite::Message::Pong(_) => continue,
                        tokio_tungstenite::tungstenite::Message::Close(_) => {
                            tx.emit(WsMessage::Close).await?;
                            return Ok(());
                        }
                        tokio_tungstenite::tungstenite::Message::Frame(_) => continue,
                    };
                    tx.emit(ws_msg).await?;
                }
                Err(e) => {
                    return Err(PipeError::Custom(Box::new(e)));
                }
            }
        }
        Ok(())
    });

    Ok((incoming, sender))
}
