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
    pub fn value(&self, row: u32) -> Option<&T> {
        (row <= self.max_rows).then(|| unsafe { self.triee.value_unchecked(row) })
    }

    #[inline(always)]
    pub fn allocate(&mut self, row: NonZeroU32) {
        let row = row.get();
        if row > self.max_rows {
            self.resize_to(row);
        }
    }

    #[inline(always)]
    pub fn create_row(&mut self) -> u32 {
        let row = self.max_rows + 1;
        self.resize_to(row);
        row
    }

    #[inline(always)]
    pub fn insert(&mut self, value: T) -> u32
    where
        T: Ord + Clone,
    {
        let row = self.create_row();
        unsafe {
            self.triee.update(row, value);
        }
        row
    }

    #[inline(always)]
    pub fn update(&mut self, row: u32, value: T)
    where
        T: Ord + Clone,
    {
        assert!(row > 0);
        self.allocate(unsafe { NonZeroU32::new_unchecked(row) });
        unsafe {
            self.triee.update(row, value);
        }
    }

    #[inline(always)]
    pub fn delete(&mut self, row: u32) {
        if row <= self.max_rows {
            unsafe { self.triee.delete(row) };
            if row == self.max_rows {
                let mut current = row - 1;
                if current >= 1 {
                    while let None = self.value(current) {
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
    pub fn exists(&self, row: u32) -> bool {
        row <= self.max_rows && unsafe { self.triee.node(row) }.is_some()
    }

    #[inline(always)]
    fn resize_to(&mut self, rows: u32) {
        let size = Self::UNIT_SIZE * (rows + 1) as u64;
        self.mmap.set_len(size).unwrap();
        self.triee = Avltriee::new(self.mmap.as_ptr() as *mut AvltrieeNode<T>);
        self.max_rows = rows;
    }
}
