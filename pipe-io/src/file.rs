//! File I/O constructors.
//!
//! ```ignore
//! use pipe_io::file;
//!
//! let pipe = file::lines("input.txt");
//! let result = pipe.map(|l| l.to_uppercase()).collect().await?;
//! ```

use std::path::{Path, PathBuf};

use pipe::pipeline::Pipe;
use pipe::pull::{ChunkFut, PullOperator};
use tokio::io::AsyncReadExt;

/// Lazy file reader that opens the file on first pull.
struct LazyFileReader {
    path: PathBuf,
    buf_size: usize,
    file: Option<tokio::fs::File>,
    buf: Vec<u8>,
    opened: bool,
}

impl PullOperator<Vec<u8>> for LazyFileReader {
    fn next_chunk(&mut self) -> ChunkFut<'_, Vec<u8>> {
        Box::pin(async move {
            if !self.opened {
                self.file = Some(tokio::fs::File::open(&self.path).await?);
                self.buf = vec![0u8; self.buf_size];
                self.opened = true;
            }
            let file = self.file.as_mut().unwrap();
            let n = file.read(&mut self.buf).await?;
            if n == 0 {
                Ok(None)
            } else {
                Ok(Some(vec![self.buf[..n].to_vec()]))
            }
        })
    }
}

/// Read a file as a stream of byte chunks.
///
/// The file is opened lazily on first pull. Returns `Pipe<Vec<u8>>`
/// which supports `.lines()` for line-oriented processing.
pub fn read(path: impl AsRef<Path>) -> Pipe<Vec<u8>> {
    let path = path.as_ref().to_owned();
    read_sized(path, 8192)
}

/// Read a file with a custom buffer size.
pub fn read_sized(path: impl Into<PathBuf>, buf_size: usize) -> Pipe<Vec<u8>> {
    let path = path.into();
    Pipe::from_pull_once(LazyFileReader {
        path,
        buf_size,
        file: None,
        buf: Vec::new(),
        opened: false,
    })
}

/// Read a file as a stream of lines.
///
/// Shorthand for `file::read(path).lines()`.
pub fn lines(path: impl AsRef<Path>) -> Pipe<String> {
    read(path).lines()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_file_bytes() {
        let path = tempfile("bytes", b"hello world");

        let result = read(&path).collect().await.unwrap();
        let flat: Vec<u8> = result.into_iter().flatten().collect();
        assert_eq!(flat, b"hello world");
    }

    #[tokio::test]
    async fn read_file_lines() {
        let path = tempfile("lines", b"line1\nline2\nline3");

        let result = lines(&path).collect().await.unwrap();
        assert_eq!(result, vec!["line1", "line2", "line3"]);
    }

    #[tokio::test]
    async fn read_file_lines_with_transform() {
        let path = tempfile("transform", b"hello\nworld");

        let result = lines(&path)
            .map(|l| l.to_uppercase())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["HELLO", "WORLD"]);
    }

    #[tokio::test]
    async fn read_nonexistent_file_errors() {
        let result = read("/tmp/pipe-io-no-such-file-12345.txt")
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_directory_errors() {
        let result = read("/tmp").collect().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_empty_file() {
        let path = tempfile("empty", b"");
        let result = read(&path).collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn lines_empty_file() {
        let path = tempfile("empty_lines", b"");
        let result = lines(&path).collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn lines_with_mixed_endings() {
        let path = tempfile("mixed", b"a\nb\r\nc\n");
        let result = lines(&path).collect().await.unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn read_sized_custom_buffer() {
        let path = tempfile("sized", b"abcdefghij");
        let chunks = read_sized(&path, 3).collect().await.unwrap();
        // Each chunk is at most 3 bytes
        for chunk in &chunks {
            assert!(chunk.len() <= 3);
        }
        let flat: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(flat, b"abcdefghij");
    }

    fn tempfile(name: &str, content: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("pipe-io-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{name}.txt"));
        std::fs::write(&path, content).unwrap();
        path
    }
}
