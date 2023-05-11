use std::{io, mem::size_of, path::Path};

pub use anyhow;
use anyhow::Result;

use avltriee::AvltrieeNode;
pub use avltriee::{Avltriee, AvltrieeHolder, AvltrieeIter, Found};
use file_mmap::FileMmap;

pub struct IdxFile<T> {
    mmap: FileMmap,
    triee: Avltriee<T>,
    max_rows: u32,
}
impl<T> IdxFile<T> {
    const UNIT_SIZE: u64 = size_of::<AvltrieeNode<T>>() as u64;

    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(Self::UNIT_SIZE)?;
        }
        let triee = Avltriee::new(filemmap.as_ptr() as *mut AvltrieeNode<T>);
        let max_rows = Self::calc_max_rows(filemmap.len()?);
        Ok(IdxFile {
            mmap: filemmap,
            triee,
            max_rows,
        })
    }
    pub fn triee(&self) -> &Avltriee<T> {
        &self.triee
    }
    pub fn triee_mut(&mut self) -> &mut Avltriee<T> {
        &mut self.triee
    }
    pub fn value(&self, row: u32) -> Option<&T> {
        if row <= self.max_rows {
            unsafe { self.triee.value(row) }
        } else {
            None
        }
    }

    pub fn new_row(&mut self, row: u32) -> io::Result<u32> {
        self.expand_to(if row != 0 { row } else { self.max_rows + 1 })
    }

    pub fn insert(&mut self, value: T) -> Result<u32>
    where
        T: Ord + Clone,
    {
        self.update(0, value)
    }
    pub fn update(&mut self, row: u32, value: T) -> Result<u32>
    where
        T: Ord + Clone,
    {
        let row = self.new_row(row)?;
        unsafe { self.triee.update(row, value)? }
        Ok(row)
    }

    pub fn delete(&mut self, row: u32) -> io::Result<()> {
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
                self.resize_to(Self::UNIT_SIZE * (current + 1) as u64)?;
            }
        }
        Ok(())
    }

    pub fn exists(&self, row: u32) -> bool {
        let mut exists = false;
        if row <= self.max_rows {
            if let Some(_) = unsafe { self.triee.node(row) } {
                exists = true;
            }
        }
        exists
    }

    fn expand_to(&mut self, record_count: u32) -> io::Result<u32> {
        let size = Self::UNIT_SIZE * (record_count + 1) as u64;
        if self.mmap.len()? < size {
            self.resize_to(size)?;
        }
        Ok(record_count)
    }

    fn resize_to(&mut self, size: u64) -> io::Result<()> {
        self.mmap.set_len(size)?;
        self.triee = Avltriee::new(self.mmap.as_ptr() as *mut AvltrieeNode<T>);
        self.max_rows = Self::calc_max_rows(size);
        Ok(())
    }

    fn calc_max_rows(file_len: u64) -> u32 {
        (file_len / Self::UNIT_SIZE) as u32 - 1
    }
}
