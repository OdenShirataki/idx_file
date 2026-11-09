//! Compares IdxFile against a simple model (row -> value map) under random
//! insert / update / delete operations, with several allocation lots and
//! after reopening the file.

use std::{collections::BTreeMap, num::NonZeroU32, path::Path};

use idx_file::{AvltrieeSearch, AvltrieeUpdate, IdxFile};

struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u32) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) % n as u64) as u32
    }
}

fn check(idx: &IdxFile<u128>, model: &BTreeMap<u32, u128>) {
    for (&row, &value) in model {
        assert_eq!(
            idx.value(NonZeroU32::new(row).unwrap()),
            Some(&value),
            "row {row}"
        );
    }
    let rows: Vec<u32> = idx.iter().map(|r| r.get()).collect();
    assert_eq!(rows.len(), model.len());
    let values: Vec<u128> = rows.iter().map(|r| model[r]).collect();
    assert!(values.windows(2).all(|w| w[0] <= w[1]), "not sorted");
}

fn run(path: &Path, seed: u64, allocation_lot: u32) {
    let _ = std::fs::remove_file(path);
    let mut rng = Lcg(seed);
    let mut model = BTreeMap::new();
    {
        let mut idx: IdxFile<u128> = IdxFile::new(path, allocation_lot);
        for _ in 0..1000 {
            let value = rng.next(30) as u128;
            match rng.next(10) {
                0..=5 => {
                    let row = idx.insert(&value);
                    model.insert(row.get(), value);
                }
                6..=7 if !model.is_empty() => {
                    let keys: Vec<u32> = model.keys().copied().collect();
                    let row = keys[rng.next(keys.len() as u32) as usize];
                    idx.update(NonZeroU32::new(row).unwrap(), &value);
                    model.insert(row, value);
                }
                _ if !model.is_empty() => {
                    let keys: Vec<u32> = model.keys().copied().collect();
                    let row = keys[rng.next(keys.len() as u32) as usize];
                    idx.delete(NonZeroU32::new(row).unwrap());
                    model.remove(&row);
                }
                _ => {}
            }
            check(&idx, &model);
        }
    }

    let mut idx: IdxFile<u128> = IdxFile::new(path, allocation_lot);
    check(&idx, &model);
    for _ in 0..200 {
        let value = rng.next(30) as u128;
        let row = idx.insert(&value);
        assert!(model.insert(row.get(), value).is_none(), "row {row} reused");
        check(&idx, &model);
    }
}

#[test]
fn random_operations_match_model() {
    let dir = std::env::temp_dir().join("idx_file-model-test");
    std::fs::create_dir_all(&dir).unwrap();
    for allocation_lot in [1, 7, 100] {
        for seed in 0..5 {
            run(
                &dir.join(format!("model-{allocation_lot}-{seed}.i")),
                seed,
                allocation_lot,
            );
        }
    }
}

#[test]
fn value_of_missing_rows_is_none() {
    let dir = std::env::temp_dir().join("idx_file-missing-test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("missing.i");
    let _ = std::fs::remove_file(&path);

    let mut idx: IdxFile<u32> = IdxFile::new(&path, 10);
    for i in 1..=3 {
        idx.insert(&i);
    }
    for row in [4, 11, 1_000_000, u32::MAX] {
        assert_eq!(idx.value(NonZeroU32::new(row).unwrap()), None, "row {row}");
    }
}
