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
Pipe::generate(|tx| async { ... })     // async generator via Emitter
Pipe::from_reader(reader)              // AsyncRead -> Pipe<Vec<u8>>
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
.scan(init, f)                         // stateful transform
.tap(f) / .inspect(f)                  // side-effect, pass through
.take(n) / .skip(n)                    // first/drop N elements
.take_while(p) / .skip_while(p)        // predicate-based slicing
.enumerate()                           // (index, element) pairs
.intersperse(separator)                // insert between elements
.chunks(size)                          // group into Vec<B>
.sliding_window(size)                  // overlapping windows
.chain(other)                          // sequential composition
.interleave(other)                     // deterministic round-robin
.zip(other) / .zip_with(other, f)      // positional pairing
.flatten()                             // Pipe<Pipe<B>> -> Pipe<B>
.unchunks()                            // Pipe<Vec<B>> -> Pipe<B>
.attempt()                             // errors -> Result elements
.none_terminate() / .un_none_terminate() // Option-based termination
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
```

### Error handling

```rust
.handle_error_with(|e| fallback_pipe)  // switch to fallback on error
.attempt()                             // errors -> Result elements
Pipe::retry(factory, max_retries)      // retry from scratch on error
```

### Terminals

```rust
.collect().await?                      // -> Vec<B>
.fold(init, f).await?                  // -> single value
.count().await?                        // -> usize
.for_each(f).await?                    // side-effect, discard
.first().await? / .last().await?       // -> Option<B>
.into_writer(writer).await?            // drain to AsyncWrite
.into_stream()                         // -> impl Stream<Item = Result<B>>
.drain_to(&sink).await?                // consume via Sink
```

### Composition

```rust
.through(f)                            // stream-level transform
.apply(&transform)                     // apply a Transform<A, B>
Transform::new(f).and_then(other)      // compose transforms
Sink::collect() / Sink::count() / ...  // reusable output destinations
```

## Examples

### File processing

```rust
use tokio::fs::File;

let reader = File::open("input.txt").await?;
let writer = File::create("output.txt").await?;

Pipe::from_reader(reader)
    .lines()
    .filter(|line| !line.is_empty())
    .map(|line| format!("{}\n", line.to_uppercase()))
    .into_writer(writer)
    .await?;
```

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

impl PullOperator<Row> for SqlCursor {
    fn next_chunk(&mut self) -> ChunkFut<'_, Row> {
        Box::pin(async move {
            let rows = self.fetch_next_batch().await?;
            if rows.is_empty() { Ok(None) } else { Ok(Some(rows)) }
        })
    }
}

// Cloneable via factory
let pipe = Pipe::from_pull(|| Box::new(SqlCursor::new(conn, query)));
```

### Async generator

```rust
let pipe = Pipe::generate(|tx| async move {
    for i in 0..100 {
        tx.emit(i).await?;
    }
    Ok(())
});
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

MIT OR Apache-2.0
