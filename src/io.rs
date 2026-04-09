//! Generic I/O adapters for [`Pipe`](crate::pipeline::Pipe).
//!
//! Sources read from any [`AsyncRead`] into a `Pipe<Vec<u8>>`.
//! Sinks drain any `Pipe<B>` where `B: AsRef<[u8]>` into an [`AsyncWrite`].
//!
//! ```ignore
//! use tokio::fs::File;
//! use tokio::net::TcpStream;
//! use pipe::prelude::*;
//!
//! // File → uppercase → TCP
//! let reader = File::open("input.txt").await?;
//! let writer = TcpStream::connect("127.0.0.1:9000").await?;
//!
//! Pipe::from_reader(reader)
//!     .lines()
//!     .map(|l| l.to_uppercase())
//!     .into_writer(writer)
//!     .await?;
//! ```

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::pull::{ChunkFut, PipeError, PullOperator};

const DEFAULT_BUF_SIZE: usize = 8192;

// ══════════════════════════════════════════════════════
// Source: AsyncRead → Pipe<Vec<u8>>
// ══════════════════════════════════════════════════════

pub(crate) struct PullReader<R> {
    reader: R,
    buf: Vec<u8>,
}

impl<R> PullReader<R> {
    pub(crate) fn new(reader: R, buf_size: usize) -> Self {
        Self {
            reader,
            buf: vec![0u8; buf_size],
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> PullOperator<Vec<u8>> for PullReader<R> {
    fn next_chunk(&mut self) -> ChunkFut<'_, Vec<u8>> {
        Box::pin(async move {
            let n = self.reader.read(&mut self.buf).await.map_err(|e| -> PipeError {
                Box::new(e)
            })?;
            if n == 0 {
                Ok(None)
            } else {
                // Copy only the bytes read into a new owned buffer
                Ok(Some(vec![self.buf[..n].to_vec()]))
            }
        })
    }
}

// ══════════════════════════════════════════════════════
// Sink: Pipe<B> → AsyncWrite
// ══════════════════════════════════════════════════════

pub(crate) async fn drain_to_writer<B, W>(
    root: &mut dyn PullOperator<B>,
    mut writer: W,
) -> Result<u64, PipeError>
where
    B: AsRef<[u8]> + Send + 'static,
    W: AsyncWrite + Unpin + Send,
{
    let mut total: u64 = 0;
    while let Some(chunk) = root.next_chunk().await? {
        for item in &chunk {
            let bytes = item.as_ref();
            writer
                .write_all(bytes)
                .await
                .map_err(|e| -> PipeError { Box::new(e) })?;
            total += bytes.len() as u64;
        }
    }
    writer
        .flush()
        .await
        .map_err(|e| -> PipeError { Box::new(e) })?;
    Ok(total)
}

// ══════════════════════════════════════════════════════
// Lines: Pipe<Vec<u8>> → Pipe<String>
// ══════════════════════════════════════════════════════

pub(crate) struct PullLines {
    pub(crate) child: Box<dyn PullOperator<Vec<u8>>>,
    remainder: Vec<u8>,
    done: bool,
}

impl PullLines {
    pub(crate) fn new(child: Box<dyn PullOperator<Vec<u8>>>) -> Self {
        Self {
            child,
            remainder: Vec::new(),
            done: false,
        }
    }
}

impl PullOperator<String> for PullLines {
    fn next_chunk(&mut self) -> ChunkFut<'_, String> {
        Box::pin(async move {
            loop {
                if self.done {
                    if self.remainder.is_empty() {
                        return Ok(None);
                    }
                    // Yield final line without trailing newline
                    let line = String::from_utf8_lossy(&self.remainder).into_owned();
                    self.remainder.clear();
                    return Ok(Some(vec![line]));
                }

                match self.child.next_chunk().await? {
                    Some(buffers) => {
                        for buf in buffers {
                            self.remainder.extend_from_slice(&buf);
                        }

                        let mut lines = Vec::new();
                        let mut start = 0;
                        while let Some(rel_pos) = self.remainder[start..].iter().position(|&b| b == b'\n') {
                            let newline = start + rel_pos;
                            // Strip \r\n or \n
                            let end = if newline > start && self.remainder[newline - 1] == b'\r' {
                                newline - 1
                            } else {
                                newline
                            };
                            lines.push(
                                String::from_utf8_lossy(&self.remainder[start..end]).into_owned(),
                            );
                            start = newline + 1;
                        }
                        // Remove consumed bytes in one operation
                        if start > 0 {
                            self.remainder.drain(..start);
                        }

                        if !lines.is_empty() {
                            return Ok(Some(lines));
                        }
                        // No complete line yet — keep reading
                    }
                    None => {
                        self.done = true;
                        // Loop back to handle remaining bytes
                    }
                }
            }
        })
    }
}

// ══════════════════════════════════════════════════════
// Pipe API extensions
// ══════════════════════════════════════════════════════

use crate::pipeline::Pipe;

impl Pipe<Vec<u8>> {
    /// Create a pipe from any [`AsyncRead`] source.
    ///
    /// Each element is a `Vec<u8>` buffer read from the source.
    /// Uses an 8 KiB read buffer by default.
    ///
    /// The resulting pipe is single-use — cloning and materializing
    /// both clones will panic. Use a factory closure with
    /// [`from_pull`](Pipe::from_pull) for cloneable I/O pipes.
    pub fn from_reader(reader: impl AsyncRead + Unpin + Send + 'static) -> Self {
        Self::from_reader_sized(reader, DEFAULT_BUF_SIZE)
    }

    /// Create a pipe from any [`AsyncRead`] with a custom buffer size.
    pub fn from_reader_sized(
        reader: impl AsyncRead + Unpin + Send + 'static,
        buf_size: usize,
    ) -> Self {
        Pipe::from_pull_once(PullReader::new(reader, buf_size))
    }

    /// Split byte buffers into lines (`\n` or `\r\n` delimited).
    ///
    /// Handles lines split across read boundaries. The final line is
    /// emitted even without a trailing newline. Uses lossy UTF-8 conversion.
    pub fn lines(self) -> Pipe<String> {
        let parent = self.factory;
        Pipe::from_factory(move || Box::new(PullLines::new(parent())))
    }
}

impl<B: AsRef<[u8]> + Send + 'static> Pipe<B> {
    /// Drain this pipe into any [`AsyncWrite`] sink.
    ///
    /// Returns the total number of bytes written. Flushes on completion.
    pub async fn into_writer(self, writer: impl AsyncWrite + Unpin + Send) -> Result<u64, PipeError> {
        let mut root = self.into_pull();
        drain_to_writer(&mut *root, writer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reader_to_pipe() {
        let data = b"hello world";
        let cursor = std::io::Cursor::new(data.to_vec());
        let result = Pipe::from_reader(cursor).collect().await.unwrap();
        let flat: Vec<u8> = result.into_iter().flatten().collect();
        assert_eq!(flat, b"hello world");
    }

    #[tokio::test]
    async fn reader_small_buffer() {
        let data = b"abcdef";
        let cursor = std::io::Cursor::new(data.to_vec());
        let result = Pipe::from_reader_sized(cursor, 2).collect().await.unwrap();
        // Should produce multiple chunks of 2 bytes each
        assert!(result.len() > 1);
        let flat: Vec<u8> = result.into_iter().flatten().collect();
        assert_eq!(flat, b"abcdef");
    }

    #[tokio::test]
    async fn lines_splits_correctly() {
        let data = b"line1\nline2\nline3";
        let cursor = std::io::Cursor::new(data.to_vec());
        let result = Pipe::from_reader(cursor).lines().collect().await.unwrap();
        assert_eq!(result, vec!["line1", "line2", "line3"]);
    }

    #[tokio::test]
    async fn lines_handles_crlf() {
        let data = b"a\r\nb\r\nc";
        let cursor = std::io::Cursor::new(data.to_vec());
        let result = Pipe::from_reader(cursor).lines().collect().await.unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn lines_trailing_newline() {
        let data = b"x\ny\n";
        let cursor = std::io::Cursor::new(data.to_vec());
        let result = Pipe::from_reader(cursor).lines().collect().await.unwrap();
        assert_eq!(result, vec!["x", "y"]);
    }

    #[tokio::test]
    async fn lines_across_chunk_boundaries() {
        // Buffer of 3 bytes, lines longer than that
        let data = b"hello\nworld\n";
        let cursor = std::io::Cursor::new(data.to_vec());
        let result = Pipe::from_reader_sized(cursor, 3)
            .lines()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["hello", "world"]);
    }

    #[tokio::test]
    async fn writer_sink_bytes() {
        let mut output = Vec::<u8>::new();
        let written = Pipe::from_iter(vec![
            vec![1u8, 2, 3],
            vec![4, 5],
        ])
        .into_writer(&mut output)
        .await
        .unwrap();
        assert_eq!(output, vec![1, 2, 3, 4, 5]);
        assert_eq!(written, 5);
    }

    #[tokio::test]
    async fn writer_sink_strings() {
        let mut output = Vec::<u8>::new();
        let written = Pipe::from_iter(vec!["hello", " ", "world"])
            .into_writer(&mut output)
            .await
            .unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "hello world");
        assert_eq!(written, 11);
    }

    #[tokio::test]
    async fn roundtrip_reader_to_writer() {
        let input = b"line1\nline2\nline3\n";
        let cursor = std::io::Cursor::new(input.to_vec());
        let mut output = Vec::<u8>::new();

        Pipe::from_reader(cursor)
            .lines()
            .map(|l| format!("{}\n", l.to_uppercase()))
            .into_writer(&mut output)
            .await
            .unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "LINE1\nLINE2\nLINE3\n"
        );
    }
}
