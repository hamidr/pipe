# pipe

A lazy, effectful streaming library for Rust, inspired by [FS2](https://fs2.io/).

## Core types

- **`Pipe<B>`** — lazy effectful stream. Pull-based, chunked internally, element-level API externally. No data flows until a terminal is called. Implements `Clone` — each clone materializes an independent pipeline.

- **`PullOperator<B>`** — public pull protocol. `next_chunk() → Option<Vec<B>>`. Implement for custom sources. `from_pull()` accepts a factory; `from_pull_once()` wraps a single instance.

- **`Transform<A, B>`** — reusable, cloneable, composable stream transform. Named, stored, and combined via `and_then`.

- **`Sink<B, R>`** — reusable, cloneable output destination. Built-in sinks: `collect`, `count`, `first`, `last`, `drain`.

- **`Operator<A, B>`** — per-element async transform with captured state. Used with `Pipe::pipe()`.

- **`CancelToken`** — cooperative cancellation signal for graceful shutdown.

## API overview

### Constructors

```rust
Pipe::from_iter(vec![1, 2, 3])        // from iterator (B: Clone)
Pipe::once(42)                         // single element
Pipe::empty()                          // empty stream
Pipe::unfold(seed, |s| Some(next))     // lazy generation from seed
Pipe::iterate(0, |x| x + 1)           // infinite: init, f(init), ...
Pipe::repeat(42)                       // infinite: same value
Pipe::repeat_with(|| rand())           // infinite: from factory
Pipe::interval(Duration::from_secs(1)) // periodic Instant ticks
Pipe::generate(|tx| async { ... })     // async generator via Emitter
Pipe::from_reader(reader)              // AsyncRead → Pipe<Vec<u8>>
Pipe::from_stream(stream)              // futures::Stream → Pipe
Pipe::from_pull(|| Box::new(src))      // custom PullOperator (cloneable)
Pipe::from_pull_once(src)              // custom PullOperator (single-use)
Pipe::bracket(acquire, use_fn, release) // resource-safe pipeline
```

### Element operators

```rust
.map(|x| x * 2)                       // transform (may change type)
.filter(|x| x > &10)                  // keep matching elements
.and_then(|x| if x > 0 { Some(x) } else { None }) // fused map+filter
.flat_map(|x| Pipe::from_iter(0..x))  // one-to-many expansion
.scan(0, |acc, x| { *acc += x; *acc }) // stateful transform
.tap(|x| println!("{x}"))             // side-effect, pass through
.inspect(|x| log(x))                  // alias for tap
.take(10)                              // first N elements
.skip(5)                               // drop first N
.take_while(|x| *x < 100)             // take while predicate holds
.skip_while(|x| *x < 10)              // skip while predicate holds
.enumerate()                           // (index, element) pairs
.intersperse(0)                        // insert separator between elements
.chunks(100)                           // group into fixed-size Vec<B>
.sliding_window(3)                     // overlapping windows
.attempt()                             // errors become Err elements
.none_terminate()                      // wrap in Some, emit None at end
.with_cancel(token)                    // stop on CancelToken signal
.meter_with("name", |stats| ...)       // observability hook
```

### Async operators

```rust
.eval_map(|x| async { Ok(x * 2) })    // async transform
.eval_filter(|x| async { Ok(*x > 0) }) // async predicate
.eval_tap(|x| async { log(x); Ok(()) }) // async side-effect
.par_eval_map(4, |x| async { ... })   // bounded parallel (ordered)
.par_eval_map_unordered(4, |x| async { ... }) // bounded parallel (fast)
.pipe(my_operator)                     // custom Operator<A, B>
```

### Composition

```rust
.through(|p| p.filter(...).map(...))   // stream-level transform
.apply(&transform)                     // apply a Transform<A, B>
.chain(other)                          // sequential: self then other
.zip(other)                            // positional pairing → (A, B)
.zip_with(other, |a, b| a + b)        // custom combiner
.interleave(other)                     // deterministic round-robin
.flatten()                             // Pipe<Pipe<B>> → Pipe<B>
.concurrently(background)              // run background, output from self
```

### Time-based

```rust
.timeout(Duration::from_secs(5))       // error if pull exceeds deadline
.throttle(Duration::from_millis(100))  // rate-limit: max one per interval
.debounce(Duration::from_millis(200))  // emit after quiet period
.chunks_timeout(100, Duration::from_millis(500)) // batch by count OR time
```

### Concurrency

```rust
.prefetch(4)                           // buffer N chunks ahead
Pipe::merge(vec![a, b, c])            // concurrent fan-in
.merge_with(other)                     // two-pipe shorthand
.broadcast(3, 4)                       // fan-out to N consumers
.broadcast_through(n, buf, transforms) // fan-out + per-branch transform + merge
.partition(4, 2, |x| *x as u64)       // hash-partition across N branches
.unzip(buf)                            // Pipe<(A, B)> → (Pipe<A>, Pipe<B>)
```

### Error handling

```rust
.handle_error_with(|e| fallback_pipe)  // switch to fallback on error
.attempt()                             // errors → Err elements (don't stop)
Pipe::retry(|| build_pipe(), 3)        // retry from scratch up to N times
```

### Option protocol

```rust
.none_terminate()                      // Pipe<B> → Pipe<Option<B>>, None at end
.un_none_terminate()                   // Pipe<Option<B>> → Pipe<B>, stop at None
```

### Terminals

```rust
.collect().await?                      // → Vec<B>
.fold(init, |acc, x| acc + x).await?   // → single value
.count().await?                        // → usize
.for_each(|x| process(x)).await?       // side-effect, discard
.first().await?                        // → Option<B>
.last().await?                         // → Option<B>
.into_writer(writer).await?            // drain to AsyncWrite → bytes written
.into_stream()                         // → impl Stream<Item = Result<B>>
.drain_to(&sink).await?                // consume via Sink
```

### Resource safety

```rust
Pipe::bracket(
    || Box::pin(async { Ok(open_resource().await?) }),
    |resource| Pipe::from_reader(resource),
    || cleanup(),
)
```

### Graceful shutdown

```rust
let token = CancelToken::new();
let pipe = Pipe::iterate(0, |x| x + 1)
    .with_cancel(token.clone())
    .prefetch(4)
    .map(|x| x * 2);

// Later: signal shutdown — source stops, pipeline drains
token.cancel();
```

### Transform composition

```rust
let normalize = Transform::new(|p: Pipe<f64>| p.map(|x| x / 100.0));
let clip = Transform::new(|p: Pipe<f64>| p.map(|x| x.max(0.0).min(1.0)));

let pipeline = normalize.and_then(clip);
let result = Pipe::from_iter(values).apply(&pipeline).collect().await?;
```

### Clone — reuse pipeline descriptions

```rust
let pipeline = Pipe::from_iter(1..=10)
    .filter(|x| x % 2 == 0)
    .map(|x| x * 10);

let a = pipeline.clone().collect().await?;
let b = pipeline.clone().fold(0, |a, b| a + b).await?;
let c = pipeline.first().await?;
```

### Observability

```rust
Pipe::from_iter(1..=1000)
    .filter(|x| x % 10 == 0)
    .meter_with("after-filter", |stats| {
        println!("{}: {} elements, {} chunks", stats.name, stats.elements, stats.chunks);
    })
    .map(|x| x * 2)
    .collect()
    .await?;
```

### Generic I/O

```rust
use tokio::fs::File;

// File → transform → file
let reader = File::open("input.txt").await?;
let writer = File::create("output.txt").await?;

Pipe::from_reader(reader)
    .lines()
    .filter(|line| !line.is_empty())
    .map(|line| format!("{}\n", line.to_uppercase()))
    .into_writer(writer)
    .await?;
```

### Custom PullOperator

```rust
use pipe::prelude::*;

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

// Or single-use
let pipe = Pipe::from_pull_once(SqlCursor::new(conn, query));
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

## License

MIT OR Apache-2.0
