//! TCP and UDP constructors.
//!
//! ```ignore
//! use pipe_io::net;
//!
//! // TCP server: each connection becomes a pipe
//! net::tcp_server("0.0.0.0:8080".parse()?)
//!     .map(|conn| {
//!         let (lines, writer) = conn.into_lines();
//!         lines.eval_map(move |line| {
//!             let writer = writer.clone();
//!             async move {
//!                 writer.write_all(format!("echo: {line}\n").as_bytes()).await?;
//!                 Ok(line)
//!             }
//!         })
//!     })
//!     .par_join_unbounded()
//!     .for_each(|msg| println!("{msg}"))
//!     .await?;
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use pipe::pipeline::Pipe;
use pipe::pull::PipeError;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

/// A TCP connection split into a read half and write half.
///
/// Use [`into_lines`](Self::into_lines), [`into_bytes`](Self::into_bytes),
/// or [`into_split`](Self::into_split) to access the underlying halves.
pub struct TcpConnection {
    addr: SocketAddr,
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
}

impl TcpConnection {
    /// The peer address of this connection.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl TcpConnection {
    /// Convert into a line-oriented pipe and a cloneable writer.
    ///
    /// The reader becomes a `Pipe<String>` (one element per line).
    /// The writer is wrapped in `Arc<Mutex>` for shared access.
    pub fn into_lines(self) -> (Pipe<String>, TcpWriter) {
        let pipe = Pipe::from_reader(self.reader).lines();
        let writer = TcpWriter(Arc::new(Mutex::new(self.writer)));
        (pipe, writer)
    }

    /// Convert into a byte-chunk pipe and a cloneable writer.
    pub fn into_bytes(self) -> (Pipe<Vec<u8>>, TcpWriter) {
        let pipe = Pipe::from_reader(self.reader);
        let writer = TcpWriter(Arc::new(Mutex::new(self.writer)));
        (pipe, writer)
    }

    /// Split into raw tokio halves.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        (self.reader, self.writer)
    }
}

/// Cloneable write handle for a TCP connection.
///
/// Each `write_all` call is individually atomic (mutex-protected),
/// but sequences of calls can interleave with other writers. For
/// multi-part messages, build the full message first and send it
/// in a single `write_all`.
#[derive(Clone, Debug)]
pub struct TcpWriter(Arc<Mutex<OwnedWriteHalf>>);

impl TcpWriter {
    /// Write all bytes to the connection. Mutex-protected per call.
    pub async fn write_all(&self, data: &[u8]) -> Result<(), PipeError> {
        self.0
            .lock()
            .await
            .write_all(data)
            .await
            .map_err(PipeError::from)
    }
}

/// Accept TCP connections as a stream of [`TcpConnection`].
///
/// Binds to the given address and emits one element per accepted
/// connection. Use with `.par_join_unbounded()` or `.par_join(n)`
/// to handle connections concurrently.
///
/// **Timeouts**: the accept loop itself has no timeout (it would
/// terminate the server during quiet periods). To guard against
/// slow or idle clients, apply `.timeout()` on per-connection
/// read pipes:
///
/// ```ignore
/// net::tcp_server("0.0.0.0:8080".parse()?)
///     .map(|conn| {
///         let (lines, writer) = conn.into_lines();
///         lines
///             .timeout(Duration::from_secs(30))  // per-read timeout
///             .eval_map(move |line| { /* handle */ })
///     })
///     .par_join_unbounded()
///     .for_each(|log| println!("{log}"))
///     .await?;
/// ```
pub fn tcp_server(addr: SocketAddr) -> Pipe<TcpConnection> {
    Pipe::generate_once(move |tx| async move {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let (reader, writer) = stream.into_split();
            tx.emit(TcpConnection {
                addr: peer_addr,
                reader,
                writer,
            })
            .await?;
        }
    })
}

/// A received UDP datagram.
pub struct Datagram {
    pub data: Vec<u8>,
    pub addr: SocketAddr,
}

/// Bind a UDP socket and receive datagrams as a stream.
///
/// Each element is a single datagram with its source address.
///
/// ```ignore
/// net::udp_bind("0.0.0.0:9090".parse()?)
///     .for_each(|dg| println!("{} bytes from {}", dg.data.len(), dg.addr))
///     .await?;
/// ```
pub fn udp_bind(addr: SocketAddr) -> Pipe<Datagram> {
    Pipe::generate_once(move |tx| async move {
        let socket = tokio::net::UdpSocket::bind(addr).await?;
        let mut buf = vec![0u8; 65535];
        loop {
            let (n, peer_addr) = socket.recv_from(&mut buf).await?;
            tx.emit(Datagram {
                data: buf[..n].to_vec(),
                addr: peer_addr,
            })
            .await?;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_server_accepts_connections() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Bind manually so we know the port for clients to connect to
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_pipe = Pipe::generate_once(move |tx| async move {
            for _ in 0..2 {
                let (stream, peer_addr) = listener.accept().await?;
                let (reader, writer) = stream.into_split();
                tx.emit(TcpConnection {
                    addr: peer_addr,
                    reader,
                    writer,
                })
                .await?;
            }
            Ok(())
        });

        let handle = tokio::spawn(async move {
            server_pipe
                .map(|conn| {
                    let addr = conn.addr();
                    let (lines, writer) = conn.into_lines();
                    lines.eval_map(move |line| {
                        let writer = writer.clone();
                        async move {
                            writer
                                .write_all(format!("echo: {line}\n").as_bytes())
                                .await?;
                            Ok(format!("[{addr}] {line}"))
                        }
                    })
                })
                .par_join_unbounded()
                .collect()
                .await
                .unwrap()
        });

        // Connect two clients
        let mut c1 = tokio::net::TcpStream::connect(addr).await.unwrap();
        c1.write_all(b"hello\n").await.unwrap();
        c1.shutdown().await.unwrap();
        let mut buf = vec![0u8; 256];
        let n = c1.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"echo: hello\n");

        let mut c2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        c2.write_all(b"world\n").await.unwrap();
        c2.shutdown().await.unwrap();
        let n = c2.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"echo: world\n");

        let logs = handle.await.unwrap();
        assert_eq!(logs.len(), 2);
        assert!(logs.iter().any(|l| l.contains("hello")));
        assert!(logs.iter().any(|l| l.contains("world")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_receives_datagrams() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let server_pipe = Pipe::generate_once(move |tx| async move {
            let mut buf = vec![0u8; 65535];
            for _ in 0..3 {
                let (n, peer_addr) = socket.recv_from(&mut buf).await?;
                tx.emit(Datagram {
                    data: buf[..n].to_vec(),
                    addr: peer_addr,
                })
                .await?;
            }
            Ok(())
        });

        let handle = tokio::spawn(async move {
            server_pipe
                .map(|dg| String::from_utf8_lossy(&dg.data).to_string())
                .collect()
                .await
                .unwrap()
        });

        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for msg in &["aaa", "bbb", "ccc"] {
            client.send_to(msg.as_bytes(), addr).await.unwrap();
        }

        let results = handle.await.unwrap();
        assert_eq!(results, vec!["aaa", "bbb", "ccc"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_into_bytes_returns_raw_data() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let (reader, writer) = stream.into_split();
            let conn = TcpConnection {
                addr: peer_addr,
                reader,
                writer,
            };
            let (bytes_pipe, _writer) = conn.into_bytes();
            let chunks = bytes_pipe.collect().await.unwrap();
            let flat: Vec<u8> = chunks.into_iter().flatten().collect();
            flat
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::AsyncWriteExt;
        client.write_all(b"raw bytes").await.unwrap();
        client.shutdown().await.unwrap();

        let data = handle.await.unwrap();
        assert_eq!(data, b"raw bytes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_client_disconnect_mid_stream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let (reader, writer) = stream.into_split();
            let conn = TcpConnection {
                addr: peer_addr,
                reader,
                writer,
            };
            let (lines_pipe, _writer) = conn.into_lines();
            lines_pipe.collect().await
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::AsyncWriteExt;
        client.write_all(b"hello\n").await.unwrap();
        drop(client); // disconnect mid-stream

        // Should complete without error (disconnect is just EOF)
        let result = handle.await.unwrap().unwrap();
        assert!(result.contains(&"hello".to_string()));
    }

    // Suppress unused warning for the `server` variable in the test above
    #[allow(dead_code)]
    fn _assert_tcp_server_compiles() {
        let _ = tcp_server("127.0.0.1:8080".parse().unwrap());
    }
}
