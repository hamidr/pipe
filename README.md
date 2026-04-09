# pipe

A lazy, effectful streaming library for Rust, inspired by [FS2](https://fs2.io/).

## Core types

- **`Pipe<B>`** — lazy effectful stream. Element-level ops (map, filter, flat_map, take, skip, take_while, skip_while), async ops (eval_map, eval_filter, eval_tap), composition (through, pipe), chunking (chunks, unchunks), concurrency (prefetch, merge, broadcast, partition, zip), error recovery (handle_error_with, retry). Terminals: `.collect()`, `.fold()`, `.count()`.

- **`PullOperator<B>`** — public pull protocol. `next_chunk() → Option<Vec<B>>`. Implement this for custom sources. `Pipe::from_pull()` wraps any `PullOperator<B>` as a `Pipe<B>`.

- **`Operator<A, B>`** — per-element async transform (Send, not Sync). `execute(A) → Future<B>`. Used with `Pipe::pipe()`.

- **`channel::bounded(n)`** — bounded async channel with backpressure. Foundation for prefetch/merge/broadcast/partition.

## Example

```rust
use pipe::Pipe;

let result: Vec<i32> = Pipe::from_iter(vec![1, 2, 3, 4, 5])
    .filter(|x| x % 2 == 0)
    .map(|x| x * 10)
    .collect()
    .await;

assert_eq!(result, vec![20, 40]);
```

## License

MIT OR Apache-2.0
