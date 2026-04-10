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
use tokio::io::AsyncReadExt;

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
    Pipe::generate_once(move |tx| async move {
        let mut file = tokio::fs::File::open(&path).await?;
        let mut buf = vec![0u8; buf_size];
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            tx.emit(buf[..n].to_vec()).await?;
        }
        Ok(())
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

    fn tempfile(name: &str, content: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("pipe-io-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{name}.txt"));
        std::fs::write(&path, content).unwrap();
        path
    }
}
