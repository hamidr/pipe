use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::routing::get;
use axum::Router;
use pipe_http::ws::{self, WsMessage};

async fn start_server(app: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_basic_receive() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                for i in 0..5 {
                    let _ = socket.send(Message::Text(format!("msg-{i}").into())).await;
                }
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<WsMessage> = incoming
        .filter(|m| m.is_text())
        .take(5)
        .collect()
        .await
        .unwrap();

    assert_eq!(messages.len(), 5);
    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg, &WsMessage::Text(format!("msg-{i}")));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_bidirectional_echo() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                while let Some(Ok(msg)) = socket.recv().await {
                    match msg {
                        Message::Text(t) => {
                            let echo = format!("echo:{t}");
                            if socket.send(Message::Text(echo.into())).await.is_err() {
                                break;
                            }
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, sender) = ws::connect(format!("ws://{addr}/ws"));

    let send_handle = tokio::spawn(async move {
        for i in 0..3 {
            sender.send_text(format!("hello-{i}")).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender.close().await.unwrap();
    });

    let replies: Vec<String> = incoming
        .and_then(|m| m.text())
        .take(3)
        .collect()
        .await
        .unwrap();

    send_handle.await.unwrap();
    assert_eq!(replies, vec!["echo:hello-0", "echo:hello-1", "echo:hello-2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_binary_messages() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                let _ = socket.send(Message::Binary(vec![1, 2, 3].into())).await;
                let _ = socket.send(Message::Binary(vec![4, 5].into())).await;
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<Vec<u8>> = incoming
        .and_then(|m| m.bytes())
        .collect()
        .await
        .unwrap();

    assert_eq!(messages, vec![vec![1, 2, 3], vec![4, 5]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_close_frame_received() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                let _ = socket.send(Message::Text("before-close".into())).await;
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<WsMessage> = incoming.collect().await.unwrap();

    assert!(messages.iter().any(|m| m.is_text()));
    assert!(messages.iter().any(|m| m.is_close()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_pipe_operators() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                for i in 0..10 {
                    let _ = socket.send(Message::Text(format!("{i}").into())).await;
                }
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let result: Vec<i64> = incoming
        .and_then(|m| m.text())
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|x| x % 2 == 0)
        .collect()
        .await
        .unwrap();

    assert_eq!(result, vec![0, 2, 4, 6, 8]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_cancellation_via_take() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                for i in 0..10000 {
                    if socket.send(Message::Text(format!("{i}").into())).await.is_err() {
                        break;
                    }
                }
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<String> = incoming
        .and_then(|m| m.text())
        .take(3)
        .collect()
        .await
        .unwrap();

    assert_eq!(messages.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connect_failure() {
    let (incoming, _sender) = ws::connect("ws://127.0.0.1:1/nonexistent");
    let result = incoming.collect().await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_sender_detects_closed_connection() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, sender) = ws::connect(format!("ws://{addr}/ws"));

    // Drain incoming to completion
    let _ = incoming.collect().await;

    // Give writer task time to notice the closure
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Sender should fail after connection is gone
    let result = sender.send_text("should fail").await;
    assert!(result.is_err());
}
