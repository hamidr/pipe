# pipe

A lazy, effectful streaming library for Rust, inspired by [FS2](https://fs2.io/).

## Core types

- **`Pipe<B>`** — lazy effectful stream. Pull-based, chunked internally, element-level API externally. No data flows until a terminal is called.

- **`PullOperator<B>`** — public pull protocol. `next_chunk() → Option<Vec<B>>`. Implement this for custom sources. `Pipe::from_pull()` wraps any `PullOperator<B>` as a `Pipe<B>`.

- **`Operator<A, B>`** — per-element async transform (`Send`, not `Sync`). `execute(A) → Future<B>`. Used with `Pipe::pipe()`.

- **`channel::bounded(n)`** — bounded async channel with backpressure. Foundation for `prefetch`/`merge`/`broadcast`/`partition`.

## Examples

### Basic pipeline

```rust
use pipe::Pipe;

let result: Vec<i32> = Pipe::from_iter(vec![1, 2, 3, 4, 5])
    .filter(|x| x % 2 == 0)
    .map(|x| x * 10)
    .collect()
    .await?;

assert_eq!(result, vec![20, 40]);
```

### Infinite streams

```rust
// Fibonacci sequence
let fibs = Pipe::unfold((0u64, 1u64), |s| {
    let val = s.0;
    let next = s.0.checked_add(s.1)?;
    *s = (s.1, next);
    Some(val)
})
.take(10)
.collect()
.await?;

assert_eq!(fibs, vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34]);

// iterate: init, f(init), f(f(init)), ...
let naturals = Pipe::iterate(0, |x| x + 1)
    .take(5)
    .collect()
    .await?;

assert_eq!(naturals, vec![0, 1, 2, 3, 4]);
```

### flat_map — one-to-many expansion

```rust
let result = Pipe::from_iter(1..=3)
    .flat_map(|x| Pipe::from_iter(0..x))
    .collect()
    .await?;

assert_eq!(result, vec![0, 0, 1, 0, 1, 2]);
```

### scan — stateful transform

```rust
let running_sum: Vec<i64> = Pipe::from_iter(vec![1, 2, 3, 4])
    .scan(0i64, |acc, x| { *acc += x; *acc })
    .collect()
    .await?;

assert_eq!(running_sum, vec![1, 3, 6, 10]);
```

### through — reusable stream transforms

```rust
fn double_evens(pipe: Pipe<i64>) -> Pipe<i64> {
    pipe.filter(|x| x % 2 == 0).map(|x| x * 2)
}

let result = Pipe::from_iter(1..=6)
    .through(double_evens)
    .collect()
    .await?;

assert_eq!(result, vec![4, 8, 12]);
```

### Async operators (eval_map, eval_filter, eval_tap)

```rust
let result = Pipe::from_iter(vec![1, 2, 3])
    .eval_map(|x| async move { Ok(format!("async-{x}")) })
    .collect()
    .await?;

assert_eq!(result, vec!["async-1", "async-2", "async-3"]);
```

### Custom Operator trait — stateful async transforms

```rust
use pipe::prelude::*;

#[derive(Debug)]
struct FetchScore { client: HttpClient }

impl Operator<UserId, Score> for FetchScore {
    fn execute<'a>(&'a self, id: UserId) -> PinFut<'a, Score> {
        Box::pin(async move {
            Ok(self.client.get_score(id).await?)
        })
    }
}

let scores = Pipe::from_iter(user_ids)
    .pipe(FetchScore { client })
    .collect()
    .await?;
```

### Concurrency

```rust
// prefetch — buffer N chunks ahead on a background task
let result = Pipe::from_iter(1..=1000)
    .prefetch(4)
    .map(|x| x * 2)
    .collect()
    .await?;

// merge — interleave multiple pipes concurrently
let merged = Pipe::merge(vec![
    Pipe::from_iter(vec![1, 3, 5]),
    Pipe::from_iter(vec![2, 4, 6]),
])
.collect()
.await?;

// merge_with — shorthand for two pipes
let merged = Pipe::from_iter(vec![1, 3, 5])
    .merge_with(Pipe::from_iter(vec![2, 4, 6]))
    .collect()
    .await?;

// broadcast — fan-out to N consumers (clone each element)
let branches = Pipe::from_iter(1..=100)
    .broadcast(3, 4);  // 3 branches, buffer 4 chunks each

// partition — hash-partition across N branches (no cloning)
let parts = Pipe::from_iter(0..100i64)
    .partition(4, 2, |x| *x as u64);
// parts[0] gets elements where key % 4 == 0, etc.

// zip — positional pairing
let pairs = Pipe::from_iter(vec![1, 2, 3])
    .zip(Pipe::from_iter(vec!["a", "b", "c"]))
    .collect()
    .await?;

assert_eq!(pairs, vec![(1, "a"), (2, "b"), (3, "c")]);
```

### Error recovery

```rust
// handle_error_with — switch to fallback on error
let result = some_pipe
    .handle_error_with(|_err| Pipe::from_iter(vec![fallback_value]))
    .collect()
    .await?;

// retry — rebuild the entire stream up to N times
let result = Pipe::retry(|| build_stream(), 3)
    .collect()
    .await?;
```

### Generic I/O — AsyncRead source, AsyncWrite sink

```rust
use tokio::fs::File;
use tokio::net::TcpStream;

// File → uppercase → file
let reader = File::open("input.txt").await?;
let writer = File::create("output.txt").await?;

Pipe::from_reader(reader)
    .lines()
    .map(|l| format!("{}\n", l.to_uppercase()))
    .into_writer(writer)
    .await?;

// TCP → process → TCP
let src = TcpStream::connect("source:9000").await?;
let dst = TcpStream::connect("dest:9001").await?;

Pipe::from_reader(src)
    .lines()
    .filter(|line| !line.is_empty())
    .into_writer(dst)
    .await?;

// Custom buffer size for large reads
let big_reader = File::open("large.bin").await?;
Pipe::from_reader_sized(big_reader, 64 * 1024)  // 64 KiB buffers
    .into_writer(&mut output)
    .await?;
```

### Custom PullOperator — bring your own source

```rust
use pipe::prelude::*;

struct SqlCursor { /* connection, query state */ }

impl PullOperator<Row> for SqlCursor {
    fn next_chunk(&mut self) -> ChunkFut<'_, Row> {
        Box::pin(async move {
            let rows = self.fetch_next_batch().await?;
            if rows.is_empty() { Ok(None) } else { Ok(Some(rows)) }
        })
    }
}

// No Box::new needed — from_pull accepts impl PullOperator directly
let pipe = Pipe::from_pull(SqlCursor::new(conn, query));
let results = pipe.map(|row| row.name).collect().await?;
```

### Sequential composition, enumerate, inspect

```rust
// chain — all of A, then all of B (sequential, not concurrent)
let result = Pipe::from_iter(vec![1, 2])
    .chain(Pipe::from_iter(vec![3, 4, 5]))
    .collect()
    .await?;

assert_eq!(result, vec![1, 2, 3, 4, 5]);

// enumerate — pair each element with its index
let result = Pipe::from_iter(vec!["a", "b", "c"])
    .enumerate()
    .collect()
    .await?;

assert_eq!(result, vec![(0, "a"), (1, "b"), (2, "c")]);

// inspect — alias for tap (Rust iterator convention)
Pipe::from_iter(1..=5)
    .inspect(|x| println!("saw: {x}"))
    .collect()
    .await?;
```

### Terminals

```rust
// for_each — run a side-effect, discard values
Pipe::from_iter(events)
    .for_each(|e| process(e))
    .await?;

// first / last
let head = Pipe::from_iter(1..=10).first().await?;  // Some(1)
let tail = Pipe::from_iter(1..=10).last().await?;    // Some(10)
```

### Stream interop

```rust
use futures_core::Stream;

// Any futures::Stream → Pipe
let pipe = Pipe::from_stream(some_stream)
    .filter(|x| x > &threshold)
    .map(|x| x * 2);

// Pipe → futures::Stream
let stream = Pipe::from_iter(1..=100)
    .map(|x| x * 10)
    .into_stream();
// stream implements Stream<Item = Result<i64, PipeError>>
```

## License

MIT OR Apache-2.0
