use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pipe::pipeline::Pipe;

const N: usize = 100_000;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn bench_from_iter_collect(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("from_iter -> collect (100k)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let v: Vec<i64> = Pipe::from_iter(0..N as i64).collect().await.unwrap();
                black_box(v);
            });
        });
    });
}

fn bench_map(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("map (100k)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let v: Vec<i64> = Pipe::from_iter(0..N as i64)
                    .map(|x| x * 2)
                    .collect()
                    .await
                    .unwrap();
                black_box(v);
            });
        });
    });
}

fn bench_filter(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("filter (100k)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let v: Vec<i64> = Pipe::from_iter(0..N as i64)
                    .filter(|x| x % 2 == 0)
                    .collect()
                    .await
                    .unwrap();
                black_box(v);
            });
        });
    });
}

fn bench_map_filter_chain(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("map -> filter -> map (100k)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let v: Vec<i64> = Pipe::from_iter(0..N as i64)
                    .map(|x| x * 2)
                    .filter(|x| x % 3 == 0)
                    .map(|x| x + 1)
                    .collect()
                    .await
                    .unwrap();
                black_box(v);
            });
        });
    });
}

fn bench_flat_map(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("flat_map (10k x 10)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let v: Vec<i64> = Pipe::from_iter(0..10_000i64)
                    .flat_map(|x| Pipe::from_iter(vec![x, x + 1]))
                    .collect()
                    .await
                    .unwrap();
                black_box(v);
            });
        });
    });
}

fn bench_chunks(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("chunks(1000) (100k)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let v: Vec<Vec<i64>> = Pipe::from_iter(0..N as i64)
                    .chunks(1000)
                    .collect()
                    .await
                    .unwrap();
                black_box(v);
            });
        });
    });
}

fn bench_fold(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("fold (100k)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let sum: i64 = Pipe::from_iter(0..N as i64)
                    .fold(0i64, |a, b| a + b)
                    .await
                    .unwrap();
                black_box(sum);
            });
        });
    });
}

fn bench_merge_two(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("merge 2 streams (50k each)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let a = Pipe::from_iter(0..50_000i64);
                let b_pipe = Pipe::from_iter(50_000..100_000i64);
                let v: Vec<i64> = Pipe::merge(vec![a, b_pipe]).collect().await.unwrap();
                black_box(v);
            });
        });
    });
}

criterion_group!(
    benches,
    bench_from_iter_collect,
    bench_map,
    bench_filter,
    bench_map_filter_chain,
    bench_flat_map,
    bench_chunks,
    bench_fold,
    bench_merge_two,
);
criterion_main!(benches);
