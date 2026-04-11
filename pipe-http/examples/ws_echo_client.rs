//! WebSocket echo client example.
//!
//! Connects to a WebSocket server, sends messages, and prints echoed
//! replies. Demonstrates bidirectional communication with pipe-http.
//!
//! Usage:
//!   cargo run -p pipe-http --example ws_echo_client -- <url>
//!
//! Test with a public echo server:
//!   cargo run -p pipe-http --example ws_echo_client -- wss://echo.websocket.org

use std::time::Duration;

use pipe_http::ws;

#[tokio::main]
async fn main() -> Result<(), pipe::pull::PipeError> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wss://echo.websocket.org".into());

    println!("connecting to {url}");

    let (incoming, sender) = ws::connect(&url);

    // Send messages in background
    let send_handle = tokio::spawn(async move {
        for i in 0..5 {
            println!("-> sending: hello-{i}");
            if let Err(e) = sender.send_text(format!("hello-{i}")).await {
                println!("send error: {e}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = sender.close().await;
    });

    // Print incoming messages
    incoming
        .and_then(|m| m.text())
        .take(5)
        .for_each(|text| println!("<- received: {text}"))
        .await?;

    send_handle.await.unwrap();
    println!("done");

    Ok(())
}
