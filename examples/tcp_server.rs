use std::net::SocketAddr;

use pipe::operator;
use pipe::pipeline::Pipe;
use pipe::pull::PipeError;
use pipe::topic::Topic;
use pipe_io::net::{self, TcpConnection, TcpWriter};

#[derive(Debug)]
struct Accept {
    topic: Topic<(SocketAddr, String)>,
}

#[operator]
impl Accept {
    async fn execute(
        &self,
        conn: TcpConnection,
    ) -> Result<Pipe<String>, Box<dyn std::error::Error + Send + Sync>> {
        let addr = conn.addr();
        let (lines, writer) = conn.into_lines();

        self.topic.publish((addr, format!("{addr} joined")))?;

        let inbound = lines
            .pipe(Publish { addr, topic: self.topic.clone() })
            .on_finalize({
                let topic = self.topic.clone();
                move || { let _ = topic.publish((addr, format!("{addr} left"))); }
            });

        let outbound = self.topic.subscribe()
            .filter(move |(src, _)| *src != addr)
            .pipe(Forward { writer });

        Ok(inbound.merge_with(outbound))
    }
}

#[derive(Debug)]
struct Publish {
    addr: SocketAddr,
    topic: Topic<(SocketAddr, String)>,
}

#[operator]
impl Publish {
    async fn execute(
        &self,
        line: String,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let msg = format!("{}: {line}", self.addr);
        self.topic.publish((self.addr, msg.clone()))?;
        Ok(msg)
    }
}

#[derive(Debug)]
struct Forward {
    writer: TcpWriter,
}

#[operator]
impl Forward {
    async fn execute(
        &self,
        (_addr, msg): (SocketAddr, String),
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.writer.write_all(format!("{msg}\n").as_bytes()).await?;
        Ok(msg)
    }
}

#[tokio::main]
async fn main() -> Result<(), PipeError> {
    println!("listening on 127.0.0.1:8080");

    let topic = Topic::new(256);

    net::tcp_server("127.0.0.1:8080".parse().unwrap())
        .pipe(Accept { topic })
        .par_join_unbounded()
        .for_each(|_| {})
        .await?;

    Ok(())
}
