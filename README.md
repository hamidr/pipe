# pipe

A lazy, effectful streaming library for Rust, inspired by [FS2](https://fs2.io/).

Pull-based, async, back-pressured. No data flows until a terminal is called.

```rust
use pipe::prelude::*;

let result = Pipe::from_iter(1..=10)
    .filter(|x| x % 2 == 0)
    .map(|x| x * 10)
    .take(3)
    .collect().await?;

assert_eq!(result, vec![20, 40, 60]);
```

## Getting started

Add to your `Cargo.toml`:

```toml
[dependencies]
pipe = { git = "https://github.com/hamidr/pipe" }

# Optional: I/O constructors for files, TCP, UDP
pipe-io = { git = "https://github.com/hamidr/pipe" }
```

Requires **Tokio** as the async runtime.

## Core types

| Type | Role |
|---|---|
| `Pipe<B>` | Lazy stream of `B`. Cloneable -- each clone materializes independently. |
| `PullOperator<B>` | Pull protocol (`next_chunk() -> Option<Vec<B>>`). Implement for custom sources. |
| `Transform<A, B>` | Reusable, composable, cloneable stream transform. |
| `Sink<B, R>` | Reusable, cloneable output destination. |
| `Operator<A, B>` | Per-element async transform with captured state. |
| `Topic<B>` | Pub/sub broadcast -- multiple subscribers receive the same messages. |
| `Signal<B>` | Reactive state -- holds a current value, emits changes to subscribers. |
| `CancelToken` | Cooperative cancellation for graceful shutdown. |
| `Emitter<B>` | Yield handle for async generator patterns. |

## API quick reference

### Constructors

```rust
Pipe::from_iter(items)                 // from IntoIterator (B: Clone)
Pipe::once(item)                       // single element
Pipe::empty()                          // empty stream
Pipe::unfold(seed, |s| Some(next))     // lazy generation from seed
Pipe::iterate(init, |x| x + 1)        // infinite: init, f(init), ...
Pipe::repeat(value)                    // infinite constant
Pipe::repeat_with(|| factory())        // infinite from factory
Pipe::interval(duration)               // periodic Instant ticks
Pipe::generate(|tx| async { ... })     // async generator via Emitter (cloneable)
Pipe::generate_once(|tx| async { ... }) // async generator (single-use, owns captures)
Pipe::from_reader(reader)              // AsyncRead -> Pipe<Vec<u8>> (8 KiB buffer)
Pipe::from_reader_sized(reader, size)  // AsyncRead with custom buffer size
Pipe::from_stream(stream)              // futures::Stream -> Pipe
Pipe::from_pull(factory)               // custom PullOperator (cloneable)
Pipe::from_pull_once(op)               // custom PullOperator (single-use)
Pipe::bracket(acquire, use_fn, release) // resource-safe pipeline
Pipe::retry(factory, max_retries)      // retry from scratch on error
```

### Element operators

```rust
.map(f)                                // transform (may change type)
.filter(predicate)                     // keep matching elements
.and_then(f)                           // fused map + filter via Option
.flat_map(f)                           // one-to-many expansion
.switch_map(f)                         // latest-wins: cancel previous inner pipe
.scan(init, f)                         // stateful transform
.tap(f) / .inspect(f)                  // side-effect, pass through
.take(n) / .skip(n)                    // first/drop N elements
.take_while(p) / .skip_while(p)        // predicate-based slicing
.enumerate()                           // (index, element) pairs
.changes()                             // emit only when value changes
.distinct() / .distinct_by(key)        // deduplicate all (via HashSet)
.group_adjacent_by(key)                // group consecutive same-key runs
.intersperse(separator)                // insert between elements
.chunks(size)                          // group into Vec<B>
.sliding_window(size)                  // overlapping windows
.chain(other)                          // sequential composition
.interleave(other)                     // deterministic round-robin
.zip(other) / .zip_with(other, f)      // positional pairing
.flatten()                             // Pipe<Pipe<B>> -> Pipe<B> (sequential)
.unchunks()                            // Pipe<Vec<B>> -> Pipe<B>
.attempt()                             // errors -> Result elements
.none_terminate() / .un_none_terminate() // Option-based termination
.on_finalize(f)                        // cleanup on completion/error/drop
.with_cancel(token)                    // stop on CancelToken signal
.meter_with(name, callback)            // observability hook
```

### Async operators

```rust
.eval_map(f)                           // async transform
.eval_filter(f)                        // async predicate
.eval_tap(f)                           // async side-effect
.par_eval_map(concurrency, f)          // bounded parallel (ordered)
.par_eval_map_unordered(concurrency, f) // bounded parallel (unordered)
.pipe(operator)                        // custom Operator<A, B>
```

### Time-based

```rust
.timeout(duration)                     // error if pull exceeds deadline
.throttle(duration)                    // rate-limit output
.delay_by(duration)                    // delay each element
.debounce(duration)                    // emit after quiet period
.chunks_timeout(max_size, duration)    // batch by count or time
```

### Concurrency

```rust
.prefetch(n)                           // buffer N chunks ahead
Pipe::merge(vec![a, b, c])            // concurrent fan-in
.merge_with(other)                     // two-pipe merge shorthand
.broadcast(n, buffer_size)             // fan-out to N consumers
.broadcast_through(n, buf, transforms) // fan-out + transform + merge
.partition(n, buf, key_fn)             // hash-partition across N branches
.unzip(buffer_size)                    // Pipe<(A, B)> -> (Pipe<A>, Pipe<B>)
.concurrently(background)              // run background alongside self
.par_join(n)                           // Pipe<Pipe<B>> -> Pipe<B> (bounded concurrency)
.par_join_unbounded()                  // Pipe<Pipe<B>> -> Pipe<B> (unlimited)
```

### Error handling

```rust
.handle_error_with(|e| fallback_pipe)  // switch to fallback on error
.attempt()                             // errors -> Result elements
Pipe::bracket(acquire, use_fn, release) // guaranteed cleanup via Arc<R>
Pipe::retry(factory, max_retries)      // retry from scratch on error
```

### Terminals

```rust
.collect().await?                      // -> Vec<B>
.fold(init, f).await?                  // -> single value
.reduce(f).await?                      // -> Option<B> (no initial value)
.count().await?                        // -> usize
.for_each(f).await?                    // side-effect, discard
.eval_for_each(f).await?               // async side-effect
.first().await? / .last().await?       // -> Option<B>
.into_writer(writer).await?            // drain to AsyncWrite
.into_stream()                         // -> impl Stream<Item = Result<B>>
.drain_to(&sink).await?                // consume via Sink
```

### Reactive primitives

```rust
// Topic: pub/sub broadcast
let topic = Topic::new(256);
let sub1 = topic.subscribe();          // Pipe<B>
let sub2 = topic.subscribe();          // independent subscriber
topic.publish(value)?;                 // broadcast to all subscribers
topic.close();                         // subscribers complete

// Signal: reactive state
let signal = Signal::new(0);
let changes = signal.subscribe();      // Pipe<B>, starts with current value
signal.set(42);                        // notify all subscribers
signal.modify(|v| *v += 1);           // mutate in place, notify subscribers
signal.get()                           // read current value

// Convert pipe to signal
let sig = some_pipe.hold(initial);     // track latest value
```

### Composition

```rust
.through(f)                            // stream-level transform
.apply(&transform)                     // apply a Transform<A, B>
Transform::new(f).and_then(other)      // compose transforms
Sink::new(f)                           // custom sink from async fn
Sink::collect()                        // -> Vec<B>
Sink::count()                          // -> usize
Sink::first() / Sink::last()          // -> Option<B>
Sink::drain()                          // discard all elements
```

## Macros

### `pipe![]` -- literal pipe construction

```rust
let p = pipe![1, 2, 3];
let result = p.filter(|x| x % 2 != 0).collect().await?;
assert_eq!(result, vec![1, 3]);
```

### `pipe_gen!` / `pipe_gen_once!` -- async generators

`pipe_gen!` wraps `Pipe::generate` (cloneable, captures must be `Clone`).
`pipe_gen_once!` wraps `Pipe::generate_once` (single-use, owns captures).

```rust
// Cloneable generator
let p = pipe_gen!(tx => {
    for i in 0..5 { tx.emit(i).await?; }      // single element
    tx.emit_all(vec![10, 20, 30]).await?;      // batch
});

// Single-use generator (can capture owned resources)
let listener = TcpListener::bind("0.0.0.0:8080").await?;
let p = pipe_gen_once!(tx => {
    loop {
        let (stream, _) = listener.accept().await?;
        tx.emit(stream).await?;
    }
});
```

### `#[operator]` -- derive `Operator<A, B>`

```rust
#[derive(Debug)]
struct Double;

#[operator]
impl Double {
    async fn execute(&self, x: i64) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(x * 2)
    }
}

let result = pipe![1, 2, 3].pipe(Double).collect().await?;
assert_eq!(result, vec![2, 4, 6]);
```

### `#[pull_operator]` -- derive `PullOperator<B>`

```rust
struct Countdown { n: usize }

#[pull_operator]
impl Countdown {
    async fn next_chunk(&mut self) -> Result<Option<Vec<usize>>, PipeError> {
        if self.n == 0 { return Ok(None); }
        let chunk = (1..=self.n).collect();
        self.n = 0;
        Ok(Some(chunk))
    }
}

let result = Pipe::from_pull_once(Countdown { n: 3 }).collect().await?;
assert_eq!(result, vec![1, 2, 3]);
```

### `#[pipe_fn]` -- async function to pipeline operator

Normalizes an async function to return `PipeResult<B>`. Bare returns are
auto-wrapped in `Ok`; `Result`/`PipeResult` returns pass through.

```rust
use pipe::prelude::*;

// Infallible -- bare return, auto-wrapped in Ok
#[pipe_fn]
async fn double(x: i64) -> i64 { x * 2 }

// Fallible -- returns PipeResult
#[pipe_fn]
async fn parse(s: String) -> PipeResult<i64> { Ok(s.parse()?) }

// Use with eval_map:
let result = pipe![1, 2, 3].eval_map(double).collect().await?;
assert_eq!(result, vec![2, 4, 6]);
```

## pipe-io

The `pipe-io` crate provides ergonomic I/O constructors.

### File I/O

```rust
use pipe_io::file;

// Read lines from a file
let lines = file::lines("input.txt")
    .filter(|l| !l.is_empty())
    .map(|l| l.to_uppercase())
    .collect().await?;

// Read raw bytes
let bytes = file::read("data.bin").collect().await?;

// Read with custom buffer size
let bytes = file::read_sized("huge.bin", 64 * 1024).collect().await?;
```

### TCP server

`TcpConnection` provides three ways to access the stream:

- `conn.into_lines()` -- line-oriented `(Pipe<String>, TcpWriter)`
- `conn.into_bytes()` -- raw byte chunks `(Pipe<Vec<u8>>, TcpWriter)`
- `conn.into_split()` -- raw tokio halves `(OwnedReadHalf, OwnedWriteHalf)`

```rust
use pipe_io::net;

net::tcp_server("0.0.0.0:8080".parse()?)
    .map(|conn| {
        let addr = conn.addr();
        let (lines, writer) = conn.into_lines();
        lines.eval_map(move |line| {
            let writer = writer.clone();
            async move {
                writer.write_all(format!("echo: {line}\n").as_bytes()).await?;
                Ok(format!("[{addr}] {line}"))
            }
        })
    })
    .par_join_unbounded()
    .for_each(|log| println!("{log}"))
    .await?;
```

### Chat server (using Topic)

```rust
use pipe::topic::Topic;
use pipe_io::net;

let topic = Topic::new(256);

net::tcp_server("0.0.0.0:8080".parse()?)
    .pipe(Accept { topic })        // register + subscribe each connection
    .par_join_unbounded()          // handle all connections concurrently
    .for_each(|_| {})
    .await?;
```

### UDP

```rust
use pipe_io::net;

net::udp_bind("0.0.0.0:9090".parse()?)
    .for_each(|dg| println!("{} bytes from {}", dg.data.len(), dg.addr))
    .await?;
```

## Examples

### Graceful shutdown

```rust
let token = CancelToken::new();
let pipe = Pipe::iterate(0, |x| x + 1)
    .with_cancel(token.clone())
    .prefetch(4)
    .map(|x| x * 2);

// Later: signal shutdown -- source stops, pipeline drains
token.cancel();
```

### Custom source

```rust
struct SqlCursor { /* ... */ }

#[pull_operator]
impl SqlCursor {
    async fn next_chunk(&mut self) -> Result<Option<Vec<Row>>, PipeError> {
        let rows = self.fetch_next_batch().await?;
        if rows.is_empty() { Ok(None) } else { Ok(Some(rows)) }
    }
}

let pipe = Pipe::from_pull(|| Box::new(SqlCursor::new(conn, query)));
```

### Reusable transforms and sinks

```rust
let normalize = Transform::new(|p: Pipe<f64>| p.map(|x| x / 100.0));
let clip = Transform::new(|p: Pipe<f64>| p.map(|x| x.max(0.0).min(1.0)));
let pipeline = normalize.and_then(clip);

let result = Pipe::from_iter(values).apply(&pipeline).collect().await?;
```

### Clone -- reuse pipeline descriptions

```rust
let pipeline = Pipe::from_iter(1..=10)
    .filter(|x| x % 2 == 0)
    .map(|x| x * 10);

let a = pipeline.clone().collect().await?;
let b = pipeline.clone().fold(0, |a, b| a + b).await?;
let c = pipeline.first().await?;
```

## License

MIT
