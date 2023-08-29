use std::{
    mem::size_of,
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
        let max_rows = Self::calc_max_rows(filemmap.len());
        IdxFile {
            mmap: filemmap,
            triee,
            max_rows,
        }
    }

    pub fn value(&self, row: u32) -> Option<&T> {
        (row <= self.max_rows)
            .then(|| unsafe { self.triee.value(row) })
            .and_then(|v| v)
    }

    pub fn new_row(&mut self, row: u32) -> u32 {
        let new_row = if row != 0 { row } else { self.max_rows + 1 };
        self.expand_to(new_row);
        new_row
    }

    pub fn insert(&mut self, value: T) -> u32
    where
        T: Ord + Clone,
    {
        self.update(0, value)
    }
    pub fn update(&mut self, row: u32, value: T) -> u32
    where
        T: Ord + Clone,
    {
        let row = self.new_row(row);
        unsafe { self.triee.update(row, value) }
        row
    }

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
                self.resize_to(Self::UNIT_SIZE * (current + 1) as u64);
            }
        }
    }

    pub fn exists(&self, row: u32) -> bool {
        row <= self.max_rows && unsafe { self.triee.node(row) }.is_some()
    }

    fn expand_to(&mut self, record_count: u32) {
        let size = Self::UNIT_SIZE * (record_count + 1) as u64;
        if self.mmap.len() < size {
            self.resize_to(size);
        }
    }

    fn resize_to(&mut self, size: u64) {
        self.mmap.set_len(size).unwrap();
        self.triee = Avltriee::new(self.mmap.as_ptr() as *mut AvltrieeNode<T>);
        self.max_rows = Self::calc_max_rows(size);
    }

    fn calc_max_rows(file_len: u64) -> u32 {
        (file_len / Self::UNIT_SIZE) as u32 - 1
    }
}
