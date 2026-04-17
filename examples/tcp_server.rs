use std::net::SocketAddr;

use lazyflow::pipeline::Pipe;
use lazyflow::pull::PipeError;
use lazyflow::topic::Topic;
use lazyflow::{operator, pipe_fn};
use lazyflow_io::net::{self, TcpConnection, TcpWriter};

type ChatMsg = (SocketAddr, String);

static TOPIC: std::sync::LazyLock<Topic<ChatMsg>> = std::sync::LazyLock::new(|| Topic::new(256));

#[pipe_fn]
async fn accept(conn: TcpConnection) -> Result<Pipe<String>, PipeError> {
    let addr = conn.addr();
    let (lines, writer) = conn.into_lines();

    let _ = TOPIC.publish((addr, format!("{addr} joined")));

    let inbound = lines.pipe(Publish { addr }).on_finalize(move || {
        let _ = TOPIC.publish((addr, format!("{addr} left")));
    });

    let outbound = TOPIC
        .subscribe()
        .filter(move |(src, _)| *src != addr)
        .pipe(Forward { writer });

    Ok(inbound.merge_with(outbound))
}

#[derive(Debug)]
struct Publish {
    addr: SocketAddr,
}

#[operator]
impl Publish {
    async fn execute(&self, line: String) -> Result<String, PipeError> {
        let msg = format!("{}: {line}", self.addr);
        TOPIC.publish((self.addr, msg.clone()))?;
        Ok(msg)
    }
}

#[derive(Debug)]
struct Forward {
    writer: TcpWriter,
}

#[operator]
impl Forward {
    async fn execute(&self, (_src, msg): ChatMsg) -> Result<String, PipeError> {
        self.writer.write_all(format!("{msg}\n").as_bytes()).await?;
        Ok(msg)
    }
}

#[tokio::main]
async fn main() -> Result<(), PipeError> {
    println!("listening on 127.0.0.1:8080");

    net::tcp_server("127.0.0.1:8080".parse().unwrap())
        .eval_map(accept)
        .par_join_unbounded()
        .for_each(|_| {})
        .await?;

    Ok(())
}
