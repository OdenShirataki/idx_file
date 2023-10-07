use std::{
    mem::size_of,
    num::NonZeroU32,
    ops::{Deref, DerefMut},
    path::Path,
};

use avltriee::AvltrieeNode;
pub use avltriee::{Avltriee, AvltrieeHolder, AvltrieeIter, Found};

pub use file_mmap::FileMmap;

pub struct IdxFile<T> {
    mmap: FileMmap,
    triee: Avltriee<T>,
    max_rows: u32,
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

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let mut filemmap = FileMmap::new(path).unwrap();
        if filemmap.len() == 0 {
            filemmap.set_len(Self::UNIT_SIZE).unwrap();
        }
        let triee = Avltriee::new(filemmap.as_ptr() as *mut AvltrieeNode<T>);
        let max_rows = (filemmap.len() / Self::UNIT_SIZE) as u32 - 1;
        IdxFile {
            mmap: filemmap,
            triee,
            max_rows,
        }
    }

    #[inline(always)]
    pub fn value(&self, row: NonZeroU32) -> Option<&T> {
        (row.get() <= self.max_rows).then(|| unsafe { self.triee.value_unchecked(row) })
    }

    #[inline(always)]
    pub fn allocate(&mut self, row: NonZeroU32) {
        let row = row.get();
        if row > self.max_rows {
            self.resize_to(row);
        }
    }

    #[inline(always)]
    pub fn create_row(&mut self) -> NonZeroU32 {
        let row = self.max_rows + 1;
        self.resize_to(row);
        unsafe { NonZeroU32::new_unchecked(row) }
    }

    pub async fn insert(&mut self, value: T) -> NonZeroU32
    where
        T: Send + Sync + Ord + Clone,
    {
        let row = self.create_row();
        unsafe {
            self.triee.update(row, value).await;
        }
        row
    }

    pub async fn update(&mut self, row: NonZeroU32, value: T)
    where
        T: Send + Sync + Ord + Clone,
    {
        self.allocate(row);
        unsafe {
            self.triee.update(row, value).await;
        }
    }

    #[inline(always)]
    pub fn delete(&mut self, row: NonZeroU32) {
        if row.get() <= self.max_rows {
            unsafe { self.triee.delete(row) };
            if row.get() == self.max_rows {
                let mut current = row.get() - 1;
                if current >= 1 {
                    while let None = self.value(unsafe { NonZeroU32::new_unchecked(current) }) {
                        current -= 1;
                        if current == 0 {
                            break;
                        }
                    }
                }
                self.resize_to(current);
            }
        }
    }

    #[inline(always)]
    pub fn exists(&self, row: NonZeroU32) -> bool {
        row.get() <= self.max_rows && unsafe { self.triee.node(row) }.is_some()
    }

    #[inline(always)]
    fn resize_to(&mut self, rows: u32) {
        let size = Self::UNIT_SIZE * (rows + 1) as u64;
        self.mmap.set_len(size).unwrap();
        self.triee = Avltriee::new(self.mmap.as_ptr() as *mut AvltrieeNode<T>);
        self.max_rows = rows;
    }
}

#[test]
fn test_insert_10000() {
    use avltriee::Avltriee;
    use avltriee::AvltrieeNode;

    const TEST_LENGTH: u32 = 1000000;

    let mut list: Vec<AvltrieeNode<u32>> = (0..=TEST_LENGTH)
        .map(|_| AvltrieeNode::new(0, 0, 0))
        .collect();
    let mut t = Avltriee::new(list.as_mut_ptr());

    futures::executor::block_on(async {
        for i in 1..=TEST_LENGTH {
            unsafe {
                t.update(i.try_into().unwrap(), i).await;
            }
        }
    });

    println!("OK:{}", 1000000);
}
