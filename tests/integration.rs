//! Complex integration tests exercising multiple pipe features together.

use pipe::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// ETL pipeline: read → parse → validate → transform → aggregate
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn etl_pipeline() {
    // Simulate CSV lines → parse → filter invalid → transform → sum
    let csv_data = b"name,score\nalice,85\nbob,invalid\ncharlie,92\ndiana,78\n";
    let cursor = std::io::Cursor::new(csv_data.to_vec());

    let total: i64 = Pipe::from_reader(cursor)
        .lines()
        .skip(1) // header
        .and_then(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            parts.get(1)?.parse::<i64>().ok()
        })
        .filter(|score| *score >= 80)
        .fold(0i64, |acc, score| acc + score)
        .await
        .unwrap();

    // alice(85) + charlie(92) = 177
    assert_eq!(total, 177);
}

/// Fan-out → per-branch transform → fan-in
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_fan_in() {
    let mut result = Pipe::from_iter(1..=12i64)
        .broadcast_through(
            4,
            vec![
                Box::new(|p: Pipe<i64>| p.map(|x| x * 10)),  // branch 0: multiply
                Box::new(|p: Pipe<i64>| p.map(|x| x + 100)), // branch 1: offset
                Box::new(|p: Pipe<i64>| p.filter(|x| x % 3 == 0)), // branch 2: filter
            ],
        )
        .collect()
        .await
        .unwrap();

    result.sort();
    // Branch 0: 10,20,30,...,120
    // Branch 1: 101,102,...,112
    // Branch 2: 3,6,9,12
    assert!(result.contains(&10));  // from branch 0
    assert!(result.contains(&101)); // from branch 1
    assert!(result.contains(&3));   // from branch 2
    assert_eq!(result.len(), 12 + 12 + 4);
}

/// Windowed moving average
#[tokio::test]
async fn moving_average() {
    let averages: Vec<f64> = Pipe::from_iter(vec![10.0, 20.0, 30.0, 40.0, 50.0])
        .sliding_window(3)
        .map(|window| window.iter().sum::<f64>() / window.len() as f64)
        .collect()
        .await
        .unwrap();

    assert_eq!(averages, vec![20.0, 30.0, 40.0]);
}

/// Pipeline cloning — build once, run multiple ways
#[tokio::test]
async fn clone_and_diverge() {
    let base = Pipe::from_iter(1..=20i64)
        .filter(|x| x % 2 == 0)
        .map(|x| x * x);

    let sum = base.clone().fold(0i64, |a, b| a + b).await.unwrap();
    let count = base.clone().count().await.unwrap();
    let first_3: Vec<i64> = base.take(3).collect().await.unwrap();

    assert_eq!(sum, 4 + 16 + 36 + 64 + 100 + 144 + 196 + 256 + 324 + 400);
    assert_eq!(count, 10);
    assert_eq!(first_3, vec![4, 16, 36]);
}

/// Error recovery chain: retry failing source, fallback on persistent failure
#[tokio::test]
async fn error_recovery_chain() {
    let attempts = Arc::new(AtomicI64::new(0));
    let attempts2 = attempts.clone();

    let result = Pipe::retry(
        move || {
            let n = attempts2.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                // First two attempts fail
                Pipe::from_iter(vec![1i64])
                    .eval_map(|_| async { Err("transient".into()) })
            } else {
                // Third attempt succeeds
                Pipe::from_iter(vec![10, 20, 30])
            }
        },
        3,
    )
    .handle_error_with(|_| Pipe::from_iter(vec![-1])) // fallback if retry exhausted
    .collect()
    .await
    .unwrap();

    assert_eq!(result, vec![10, 20, 30]);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

/// Attempt: catch errors as elements instead of stopping
#[tokio::test]
async fn attempt_catches_error_as_element() {
    use pipe::pull::{ChunkFut, PullOperator};

    // Source that yields [1, 2], then errors, simulating a flaky source
    struct FlakySource {
        phase: u8,
    }
    impl PullOperator<i64> for FlakySource {
        fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
            Box::pin(async move {
                match self.phase {
                    0 => { self.phase = 1; Ok(Some(vec![1, 2])) }
                    _ => Err("flaky".into()),
                }
            })
        }
    }

    let results: Vec<String> = Pipe::from_pull_once(FlakySource { phase: 0 })
        .attempt()
        .map(|r| match r {
            Ok(v) => format!("ok:{v}"),
            Err(e) => format!("err:{e}"),
        })
        .collect()
        .await
        .unwrap();

    assert_eq!(results[0], "ok:1");
    assert_eq!(results[1], "ok:2");
    assert!(results[2].starts_with("err:"));
    assert_eq!(results.len(), 3); // stream ends after error
}

/// Generate → partition → merge roundtrip with stateful generator
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generate_partition_merge() {
    let result = Pipe::generate(|tx| async move {
        for i in 0..20i64 {
            tx.emit(i).await?;
        }
        Ok(())
    })
    .partition(4, 2, |x| *x as u64)
    .into_iter()
    .collect::<Vec<_>>()
    .into_iter()
    .map(|p| p.map(|x| x * 100))
    .collect::<Vec<_>>();

    let merged = Pipe::merge(result);
    let mut all = merged.collect().await.unwrap();
    all.sort();

    let expected: Vec<i64> = (0..20).map(|x| x * 100).collect();
    assert_eq!(all, expected);
}

/// Transform composition: build reusable pipeline stages
#[tokio::test]
async fn transform_composition() {
    let normalize = Transform::new(|p: Pipe<f64>| {
        p.scan((0.0f64, 0u64), |state, x| {
            state.0 += x;
            state.1 += 1;
            x - (state.0 / state.1 as f64)
        })
    });

    let clip = Transform::new(|p: Pipe<f64>| {
        p.map(|x| x.max(-10.0).min(10.0))
    });

    let pipeline = normalize.and_then(clip);

    let result = Pipe::from_iter(vec![100.0, 200.0, 300.0])
        .apply(&pipeline)
        .collect()
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    // Values should be clipped to [-10, 10]
    assert!(result.iter().all(|x| *x >= -10.0 && *x <= 10.0));
}

/// Option-based protocol: producer sends data, signals done with None
#[tokio::test]
async fn none_terminate_protocol() {
    // Simulate a protocol where producer sends items then None
    let producer = Pipe::from_iter(vec![Some(1i64), Some(2), Some(3), None, Some(99)])
        .un_none_terminate();

    let result = producer
        .map(|x| x * 10)
        .collect()
        .await
        .unwrap();

    // Should stop at None, never see 99
    assert_eq!(result, vec![10, 20, 30]);
}

/// I/O roundtrip: read → transform → write
#[tokio::test]
async fn io_roundtrip() {
    let input = b"hello\nworld\nfoo\nbar\n";
    let cursor = std::io::Cursor::new(input.to_vec());

    let mut output = Vec::<u8>::new();

    Pipe::from_reader(cursor)
        .lines()
        .filter(|line| line.len() > 3) // drop "foo", "bar"
        .map(|line| format!("[{}]\n", line.to_uppercase()))
        .into_writer(&mut output)
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8(output).unwrap(),
        "[HELLO]\n[WORLD]\n"
    );
}

/// Zip + enumerate + scan: numbered running total of paired values
#[tokio::test]
async fn zip_enumerate_scan() {
    let prices = Pipe::from_iter(vec![10.0, 20.0, 30.0]);
    let quantities = Pipe::from_iter(vec![5.0, 3.0, 7.0]);

    let result: Vec<String> = prices
        .zip_with(quantities, |price, qty| price * qty)
        .enumerate()
        .scan(0.0f64, |total, (idx, value)| {
            *total += value;
            format!("#{}: value={}, running_total={}", idx, value, *total)
        })
        .collect()
        .await
        .unwrap();

    assert_eq!(result[0], "#0: value=50, running_total=50");
    assert_eq!(result[1], "#1: value=60, running_total=110");
    assert_eq!(result[2], "#2: value=210, running_total=320");
}

/// Bracket: resource lifecycle across pipeline operations
#[tokio::test]
async fn bracket_resource_lifecycle() {
    use std::sync::atomic::AtomicBool;

    let acquired = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let acq = acquired.clone();
    let rel = released.clone();

    let result = Pipe::bracket(
        move || {
            let acq = acq.clone();
            Box::pin(async move {
                acq.store(true, Ordering::SeqCst);
                Ok(vec![1i64, 2, 3, 4, 5])
            })
        },
        |data| {
            Pipe::from_iter((*data).clone())
                .filter(|x| x % 2 != 0)
                .map(|x| x * 100)
        },
        move |_| rel.store(true, Ordering::SeqCst),
    )
    .collect()
    .await
    .unwrap();

    assert_eq!(result, vec![100, 300, 500]);
    assert!(acquired.load(Ordering::SeqCst));
    assert!(released.load(Ordering::SeqCst));
}

/// Cancel token: graceful shutdown mid-stream
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_mid_stream() {
    let token = CancelToken::new();
    let t = token.clone();

    // Infinite stream, cancelled after processing a few elements
    let counter = Arc::new(AtomicI64::new(0));
    let c = counter.clone();

    let result = Pipe::iterate(1i64, |x| x + 1)
        .with_cancel(token)
        .tap(move |_| {
            if c.fetch_add(1, Ordering::SeqCst) >= 10 {
                t.cancel();
            }
        })
        .collect()
        .await
        .unwrap();

    // Should have stopped shortly after 10 elements
    assert!(result.len() >= 10);
    assert!(result.len() < 300); // definitely not infinite
}

/// Meter: observe pipeline stats
#[tokio::test]
async fn meter_observes_throughput() {
    use pipe::meter::MeterStats;
    use std::sync::Mutex;

    let stats_log = Arc::new(Mutex::new(Vec::<MeterStats>::new()));
    let log = stats_log.clone();

    let result = Pipe::from_iter(1..=100i64)
        .filter(|x| x % 10 == 0)
        .meter_with("after-filter", move |stats| {
            log.lock().unwrap().push(stats.clone());
        })
        .map(|x| x * 2)
        .collect()
        .await
        .unwrap();

    assert_eq!(result, vec![20, 40, 60, 80, 100, 120, 140, 160, 180, 200]);

    let snapshots = stats_log.lock().unwrap();
    let final_stats = snapshots.last().unwrap();
    assert_eq!(final_stats.elements, 10);
    assert!(final_stats.completed);
}

/// par_join: merge dynamically produced inner pipes concurrently
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn par_join_merges_concurrent_pipes() {
    use std::sync::Mutex;

    let order = Arc::new(Mutex::new(Vec::new()));
    let order2 = order.clone();

    // Outer pipe produces 5 inner pipes, each emitting [i*10, i*10+1, i*10+2]
    let result = Pipe::from_iter(0..5i64)
        .map(move |i| {
            let order = order2.clone();
            Pipe::from_iter(0..3i64)
                .map(move |j| i * 10 + j)
                .tap(move |x| order.lock().unwrap().push(*x))
        })
        .par_join(3)
        .collect()
        .await
        .unwrap();

    // All 15 elements should be present
    let mut sorted = result.clone();
    sorted.sort();
    assert_eq!(sorted, vec![0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32, 40, 41, 42]);
}

/// par_join_unbounded: all inner pipes run concurrently
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn par_join_unbounded_merges_all() {
    let result = Pipe::from_iter(0..4i64)
        .map(|i| Pipe::from_iter(vec![i * 100, i * 100 + 1]))
        .par_join_unbounded()
        .collect()
        .await
        .unwrap();

    let mut sorted = result;
    sorted.sort();
    assert_eq!(sorted, vec![0, 1, 100, 101, 200, 201, 300, 301]);
}

/// par_join respects concurrency limit
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn par_join_limits_concurrency() {
    use std::sync::atomic::AtomicUsize;

    let active = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));

    let active2 = active.clone();
    let max_seen2 = max_seen.clone();

    let result = Pipe::from_iter(0..10i64)
        .map(move |i| {
            let active = active2.clone();
            let max_seen = max_seen2.clone();
            pipe::pipe_gen_once!(tx => {
                let prev = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(prev, Ordering::SeqCst);
                tokio::task::yield_now().await;
                tx.emit(i).await?;
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
            })
        })
        .par_join(3)
        .collect()
        .await
        .unwrap();

    assert_eq!(result.len(), 10);
    // Concurrency should not exceed 3 (may be less due to timing)
    assert!(max_seen.load(Ordering::SeqCst) <= 3,
        "max concurrent was {}, expected <= 3", max_seen.load(Ordering::SeqCst));
}

/// par_join propagates errors from inner pipes (fail-fast)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn par_join_propagates_inner_error() {
    use pipe::pull::PipeError;

    let result = Pipe::from_iter(0..5i64)
        .map(|i| {
            Pipe::from_iter(vec![i]).eval_map(move |x| async move {
                if x == 3 {
                    Err(PipeError::Custom("boom".into()))
                } else {
                    tokio::task::yield_now().await;
                    Ok(x)
                }
            })
        })
        .par_join(5)
        .collect()
        .await;

    assert!(result.is_err(), "expected error but got {:?}", result);
    let err = result.unwrap_err();
    assert!(err.to_string().contains("boom"), "expected 'boom' but got: {err}");
}

/// par_join propagates errors from outer pipe
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn par_join_propagates_outer_error() {
    use pipe::pull::PipeError;

    // Outer pipe: emit one inner pipe then error
    let result = Pipe::from_iter(vec![Pipe::from_iter(vec![1i64])])
        .chain(
            Pipe::from_iter(vec![0i64]).eval_map(|_| async {
                Err::<Pipe<i64>, _>(PipeError::Custom("outer fail".into()))
            }),
        )
        .par_join_unbounded()
        .collect()
        .await;

    assert!(result.is_err());
}

/// par_join with cancel token stops cleanly
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn par_join_with_take() {
    let result = Pipe::from_iter(0..100i64)
        .map(|i| Pipe::from_iter(vec![i]))
        .par_join_unbounded()
        .take(5)
        .collect()
        .await
        .unwrap();

    assert_eq!(result.len(), 5);
}
