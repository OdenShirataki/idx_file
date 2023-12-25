use std::{
    mem::size_of,
    num::NonZeroU32,
    ops::{Deref, DerefMut},
    path::Path,
    ptr::NonNull,
};

use avltriee::AvltrieeNode;
pub use avltriee::{Avltriee, AvltrieeHolder, AvltrieeIter, Found};

pub use file_mmap::FileMmap;

pub struct IdxFile<T> {
    mmap: FileMmap,
    triee: Avltriee<T>,
    allocation_lot: u32,
    rows_capacity: u32,
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
impl<T> IdxFile<T> {
    const UNIT_SIZE: u64 = size_of::<AvltrieeNode<T>>() as u64;

    /// Opens the file and creates the IdxFile<T>.
    /// # Arguments
    /// * `path` - Path of file to save data
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        let mut filemmap = FileMmap::new(path).unwrap();
        if filemmap.len() == 0 {
            filemmap.set_len(Self::UNIT_SIZE).unwrap();
        }
        let rows_capacity = (filemmap.len() / Self::UNIT_SIZE) as u32 - 1;
        let triee = Avltriee::new(unsafe {
            NonNull::new_unchecked(filemmap.as_ptr() as *mut AvltrieeNode<T>)
        });
        Self {
            mmap: filemmap,
            triee,
            allocation_lot,
            rows_capacity,
        }
    }

    /// Gets the value of the specified row. Returns None if a non-existent row is specified.
    pub fn value(&self, row: NonZeroU32) -> Option<&T> {
        (row.get() <= self.max_rows()).then(|| unsafe { self.triee.value_unchecked(row) })
    }

    /// Expand data storage space.
    /// # Arguments
    /// * `min_capacity` - Specify the number of rows to expand. If allocation_lot is a larger value, it may be expanded by allocation_lot.
    pub fn allocate(&mut self, min_capacity: NonZeroU32) {
        if self.rows_capacity < min_capacity.get() {
            self.rows_capacity =
                (min_capacity.get() / self.allocation_lot + 1) * self.allocation_lot;
            self.mmap
                .set_len(Self::UNIT_SIZE * (self.rows_capacity + 1) as u64)
                .unwrap();
            self.triee = Avltriee::new(unsafe {
                NonNull::new_unchecked(self.mmap.as_ptr() as *mut AvltrieeNode<T>)
            });
        }
    }

    /// Add capacity for new row.
    pub fn create_row(&mut self) -> NonZeroU32 {
        let row = unsafe { NonZeroU32::new_unchecked(self.max_rows() + 1) };
        self.allocate(row);
        row
    }

    /// Creates a new row and assigns a value to it..
    pub async fn insert(&mut self, value: T) -> NonZeroU32
    where
        T: Ord + Copy,
    {
        let row = self.create_row();
        unsafe {
            self.triee.update(row, value).await;
        }
        row
    }

    /// Updates the value of the specified row. If capacity is insufficient, it will be expanded automatically.
    pub async fn update_with_allocate(&mut self, row: NonZeroU32, value: T)
    where
        T: Ord + Copy,
    {
        self.allocate(row);
        unsafe { self.triee.update(row, value).await }
    }

    /// Check if row exists.
    pub fn exists(&self, row: NonZeroU32) -> bool {
        row.get() <= self.max_rows() && unsafe { self.triee.node(row) }.is_some()
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

    idx.allocate(TEST_LENGTH.try_into().unwrap());

    futures::executor::block_on(async {
        for i in 1..=TEST_LENGTH {
            idx.insert(i).await;
        }
    });

    println!("OK:{}", idx.max_rows());
}
