use proptest::prelude::*;

use pipe::pipeline::Pipe;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #[test]
    fn partition_preserves_all_elements(ref data in prop::collection::vec(0..1000i64, 0..200)) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let n = 4;
            let branches = Pipe::from_iter(data.clone()).partition(n, 4, |x| *x as u64);
            let mut all = Vec::new();
            for branch in branches {
                all.extend(branch.collect().await.unwrap());
            }
            all.sort();
            let mut expected = data;
            expected.sort();
            prop_assert_eq!(all, expected);
            Ok(())
        })?;
    }

    #[test]
    fn chunks_unchunks_roundtrip(ref data in prop::collection::vec(0..1000i64, 0..200)) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let chunk_size = 7;
            let result = Pipe::from_iter(data.clone())
                .chunks(chunk_size)
                .unchunks()
                .collect()
                .await
                .unwrap();
            prop_assert_eq!(result, data);
            Ok(())
        })?;
    }

    #[test]
    fn distinct_is_idempotent(ref data in prop::collection::vec(0..20i64, 0..100)) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let once = Pipe::from_iter(data.clone())
                .distinct()
                .collect()
                .await
                .unwrap();
            let twice = Pipe::from_iter(data)
                .distinct()
                .distinct()
                .collect()
                .await
                .unwrap();
            prop_assert_eq!(once, twice);
            Ok(())
        })?;
    }

    #[test]
    fn map_preserves_length(ref data in prop::collection::vec(0..1000i64, 0..200)) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let result = Pipe::from_iter(data.clone())
                .map(|x| x * 2)
                .count()
                .await
                .unwrap();
            prop_assert_eq!(result, data.len());
            Ok(())
        })?;
    }

    #[test]
    fn take_skip_partition(ref data in prop::collection::vec(0..1000i64, 0..200), n in 0..100usize) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let taken = Pipe::from_iter(data.clone())
                .take(n)
                .collect()
                .await
                .unwrap();
            let skipped = Pipe::from_iter(data.clone())
                .skip(n)
                .collect()
                .await
                .unwrap();
            let mut combined = taken;
            combined.extend(skipped);
            prop_assert_eq!(combined, data);
            Ok(())
        })?;
    }

    #[test]
    fn fold_sum_matches_iterator(ref data in prop::collection::vec(0..1000i64, 0..200)) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let pipe_sum = Pipe::from_iter(data.clone())
                .fold(0i64, |a, b| a + b)
                .await
                .unwrap();
            let iter_sum: i64 = data.iter().sum();
            prop_assert_eq!(pipe_sum, iter_sum);
            Ok(())
        })?;
    }

    #[test]
    fn filter_matches_iterator(ref data in prop::collection::vec(0..1000i64, 0..200)) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let pipe_result = Pipe::from_iter(data.clone())
                .filter(|x| x % 3 == 0)
                .collect()
                .await
                .unwrap();
            let iter_result: Vec<i64> = data.into_iter().filter(|x| x % 3 == 0).collect();
            prop_assert_eq!(pipe_result, iter_result);
            Ok(())
        })?;
    }

    #[test]
    fn enumerate_produces_correct_indices(ref data in prop::collection::vec(0..1000i64, 0..200)) {
        let rt = rt();
        let data = data.clone();
        rt.block_on(async {
            let result = Pipe::from_iter(data.clone())
                .enumerate()
                .collect()
                .await
                .unwrap();
            let expected: Vec<(usize, i64)> = data.into_iter().enumerate().collect();
            prop_assert_eq!(result, expected);
            Ok(())
        })?;
    }
}
