mod allocator;

use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

use allocator::IdxFileAvltrieeAllocator;
pub use avltriee::{Avltriee, AvltrieeHolder, AvltrieeIter, Found};

pub use file_mmap::FileMmap;

pub struct IdxFile<T> {
    triee: Avltriee<T>,
}

impl<T> Deref for IdxFile<T> {
    type Target = Avltriee<T>;

    fn deref(&self) -> &Self::Target {
        &self.triee
    }
}

impl<T> DerefMut for IdxFile<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.triee
    }
}

impl<T: 'static> IdxFile<T> {
    /// Opens the file and creates the IdxFile<T>.
    /// # Arguments
    /// * `path` - Path of file to save data
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        Self {
            triee: Avltriee::with_allocator(Box::new(IdxFileAvltrieeAllocator::new(
                path,
                allocation_lot,
            ))),
        }
    }
}

#[test]
fn test_insert_10000() {
    use std::path::PathBuf;

    let dir = "./test/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();
    let path = PathBuf::from("./test/test.i".to_string());
    let mut idx: IdxFile<u32> = IdxFile::new(path, 1000000);

    const TEST_LENGTH: u32 = 1000000;

    futures::executor::block_on(async {
        for i in 1..=TEST_LENGTH {
            idx.insert(i).await;
        }
    });

    println!("OK:{}", idx.rows_count());
}
