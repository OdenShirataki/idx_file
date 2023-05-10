use std::{io, mem::size_of, path::Path};

pub use anyhow;
use anyhow::Result;

use avltriee::AvltrieeNode;
pub use avltriee::{Avltriee, AvltrieeIter, Found, UOrd};
use file_mmap::FileMmap;

pub trait RefIdxFile<T> {
    fn idx(&mut self) -> &mut IdxFile<T>;
}
pub struct IdxFile<T> {
    mmap: FileMmap,
    triee: Avltriee<T>,
}
impl<T> IdxFile<T> {
    const UNIT_SIZE: u64 = size_of::<AvltrieeNode<T>>() as u64;

    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(Self::UNIT_SIZE)?;
        }
        let triee = Avltriee::new(filemmap.as_ptr() as *mut AvltrieeNode<T>);
        Ok(IdxFile {
            mmap: filemmap,
            triee,
        })
    }
    pub fn triee(&self) -> &Avltriee<T> {
        &self.triee
    }
    pub fn triee_mut(&mut self) -> &mut Avltriee<T> {
        &mut self.triee
    }
    pub fn value(&self, row: u32) -> Option<&T> {
        if let Ok(max_rows) = self.max_rows() {
            if row <= max_rows {
                return unsafe { self.triee.value(row) };
            }
        }
        None
    }

    pub fn insert(&mut self, value: T) -> io::Result<u32>
    where
        T: Ord + Clone,
    {
        self.update(0, value)
    }
    pub fn update(&mut self, row: u32, value: T) -> io::Result<u32>
    where
        T: Ord + Clone,
    {
        let row = self.new_row(row)?;
        unsafe {
            self.triee.update(row, value);
        }
        Ok(row)
    }

    pub fn insert_unique(&mut self, value: T, found: Found) -> io::Result<u32> {
        let row = self.new_row(0)?;
        unsafe {
            self.triee.update_unique(row, value, found);
        }
        Ok(row)
    }

    pub fn insert_uord<H, I>(holder: &mut H, input: I) -> Result<u32>
    where
        T: Clone,
        H: UOrd<T, I> + RefIdxFile<T>,
    {
        Self::update_uord(holder, 0, input)
    }
    pub fn update_uord<H, I>(holder: &mut H, row: u32, input: I) -> Result<u32>
    where
        T: Clone,
        H: UOrd<T, I> + RefIdxFile<T>,
    {
        let row = holder.idx().new_row(row)?;
        unsafe {
            Avltriee::update_uord(holder, row, input)?;
        }
        Ok(row)
    }

    pub fn delete(&mut self, row: u32) -> io::Result<()> {
        if let Ok(max_rows) = self.max_rows() {
            if row <= max_rows {
                unsafe { self.triee.delete(row) };
                if row == max_rows {
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
        }
        Ok(())
    }

    pub fn exists(&self, row: u32) -> bool {
        let mut exists = false;
        if let Ok(max_rows) = self.max_rows() {
            if row <= max_rows {
                if let Some(_) = unsafe { self.triee.node(row) } {
                    exists = true;
                }
            }
        }
        exists
    }

    fn new_row(&mut self, row: u32) -> io::Result<u32> {
        let sizing_count = if row != 0 { row } else { self.max_rows()? + 1 };
        self.expand_to(sizing_count)
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
        Ok(())
    }

    fn max_rows(&self) -> io::Result<u32> {
        Ok((self.mmap.len()? / Self::UNIT_SIZE) as u32 - 1)
    }
}
