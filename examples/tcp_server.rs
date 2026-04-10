use std::net::SocketAddr;

use pipe::{pipe_fn, PipeResult};
use pipe::pipeline::Pipe;
use pipe::pull::PipeError;
use pipe::topic::Topic;
use pipe_io::net::{self, TcpConnection, TcpWriter};

type ChatMsg = (SocketAddr, String);

#[pipe_fn]
async fn accept(conn: TcpConnection) -> PipeResult<Pipe<String>> {
    let addr = conn.addr();
    let (lines, writer) = conn.into_lines();

    TOPIC.publish((addr, format!("{addr} joined")))?;

    let inbound = lines
        .pipe(Publish { addr })
        .on_finalize(move || { let _ = TOPIC.publish((addr, format!("{addr} left"))); });

    let outbound = TOPIC.subscribe()
        .filter(move |(src, _)| *src != addr)
        .pipe(Forward { writer });

    Ok(inbound.merge_with(outbound))
}

#[derive(Debug)]
struct Publish { addr: SocketAddr }

#[pipe::operator]
impl Publish {
    async fn execute(&self, line: String) -> PipeResult<String> {
        let msg = format!("{}: {line}", self.addr);
        TOPIC.publish((self.addr, msg.clone()))?;
        Ok(msg)
    }
}

#[derive(Debug)]
struct Forward { writer: TcpWriter }

#[pipe::operator]
impl Forward {
    async fn execute(&self, (_addr, msg): ChatMsg) -> PipeResult<String> {
        self.writer.write_all(format!("{msg}\n").as_bytes()).await?;
        Ok(msg)
    }
}

// Global topic -- safe because Topic is Clone + Send + Sync
static TOPIC: std::sync::LazyLock<Topic<ChatMsg>> =
    std::sync::LazyLock::new(|| Topic::new(256));

#[tokio::main]
async fn main() -> Result<(), PipeError> {
    println!("listening on 127.0.0.1:8080");

    net::tcp_server("127.0.0.1:8080".parse().unwrap())
        .pipe(Accept)
        .par_join_unbounded()
        .for_each(|_| {})
        .await?;

    Ok(())
}
