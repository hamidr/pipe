use pipe::prelude::*;
use pipe::{pipe, pipe_gen};

#[tokio::test]
async fn pipe_macro_basic() {
    let result = pipe![1, 2, 3].collect().await.unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn pipe_macro_trailing_comma() {
    let result = pipe![10, 20,].collect().await.unwrap();
    assert_eq!(result, vec![10, 20]);
}

#[tokio::test]
async fn pipe_macro_single() {
    let result = pipe![42].collect().await.unwrap();
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn pipe_macro_chained() {
    let result = pipe![1, 2, 3, 4, 5]
        .filter(|x| x % 2 == 0)
        .map(|x| x * 10)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![20, 40]);
}

#[tokio::test]
async fn pipe_gen_basic() {
    let result = pipe_gen!(tx => {
        tx.emit(1).await?;
        tx.emit(2).await?;
        tx.emit(3).await?;
    })
    .collect()
    .await
    .unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn pipe_gen_with_loop() {
    let result = pipe_gen!(tx => {
        for i in 0..5 {
            tx.emit(i * 10).await?;
        }
    })
    .map(|x| x + 1)
    .collect()
    .await
    .unwrap();
    assert_eq!(result, vec![1, 11, 21, 31, 41]);
}

#[derive(Debug)]
struct Double;

#[operator]
impl Double {
    async fn execute(&self, input: i64) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(input * 2)
    }
}

#[tokio::test]
async fn operator_macro_basic() {
    let result = pipe![1, 2, 3].pipe(Double).collect().await.unwrap();
    assert_eq!(result, vec![2, 4, 6]);
}

#[derive(Debug)]
struct Prefix {
    prefix: String,
}

#[operator]
impl Prefix {
    async fn execute(
        &self,
        input: String,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(format!("{}{}", self.prefix, input))
    }
}

#[tokio::test]
async fn operator_macro_with_state() {
    let op = Prefix {
        prefix: "hi_".to_string(),
    };
    let result = pipe!["a".to_string(), "b".to_string()]
        .pipe(op)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec!["hi_a", "hi_b"]);
}

struct Counter {
    items: Vec<i64>,
    done: bool,
}

#[pull_operator]
impl Counter {
    async fn next_chunk(&mut self) -> Result<Option<Vec<i64>>, pipe::pull::PipeError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        Ok(Some(self.items.clone()))
    }
}

#[tokio::test]
async fn pull_operator_macro_basic() {
    let src = Counter {
        items: vec![10, 20, 30],
        done: false,
    };
    let result = Pipe::from_pull_once(src).collect().await.unwrap();
    assert_eq!(result, vec![10, 20, 30]);
}

struct Countdown {
    n: usize,
}

#[pull_operator]
impl Countdown {
    async fn next_chunk(&mut self) -> Result<Option<Vec<usize>>, pipe::pull::PipeError> {
        if self.n == 0 {
            return Ok(None);
        }
        let chunk: Vec<usize> = (1..=self.n).collect();
        self.n = 0;
        Ok(Some(chunk))
    }
}

#[tokio::test]
async fn pull_operator_macro_with_pipe_ops() {
    let result = Pipe::from_pull_once(Countdown { n: 5 })
        .filter(|x| x % 2 != 0)
        .map(|x| x * 100)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![100, 300, 500]);
}
