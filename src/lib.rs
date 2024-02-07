mod allocator;

use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

pub use allocator::IdxFileAllocator;
pub use avltriee::{search, Avltriee, AvltrieeIter, AvltrieeSearch, AvltrieeUpdate};

pub use file_mmap::FileMmap;

pub type IdxFileAvlTriee<T, I> = Avltriee<T, I, IdxFileAllocator<T>>;

pub struct IdxFile<T, I: ?Sized = T> {
    triee: IdxFileAvlTriee<T, I>,
}

impl<T, I: ?Sized> Deref for IdxFile<T, I> {
    type Target = IdxFileAvlTriee<T, I>;

    fn deref(&self) -> &Self::Target {
        &self.triee
    }
}

impl<T, I: ?Sized> DerefMut for IdxFile<T, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.triee
    }
}

impl<T, I: ?Sized> IdxFile<T, I> {
    /// Opens the file and creates the IdxFile<T>.
    /// # Arguments
    /// * `path` - Path of file to save data
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        Self {
            triee: Avltriee::with_allocator(IdxFileAllocator::new(path, allocation_lot)),
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

    const TEST_LENGTH: u32 = 1000;

    for i in 1..=TEST_LENGTH {
        idx.insert(&i);
    }

    println!("iter");
    for row in idx.iter() {
        println!(" {} : {}", row, unsafe { idx.value_unchecked(row) });
    }

    println!("iter_by");
    for row in idx.iter_by(&100) {
        println!(" {} : {}", row, unsafe { idx.value_unchecked(row) });
    }

    println!("iter_from");
    for row in idx.iter_from(&100) {
        println!(" {} : {}", row, unsafe { idx.value_unchecked(row) });
    }

    println!("iter_to");
    for row in idx.iter_to(&200) {
        println!(" {} : {}", row, unsafe { idx.value_unchecked(row) });
    }

    println!("iter_range");
    for row in idx.iter_range(&100, &200) {
        println!(" {} : {}", row, unsafe { idx.value_unchecked(row) });
    }

    println!("OK:{}", idx.rows_count());
}
