use std::{marker::PhantomData, mem::size_of, path::Path};

use avltriee::{AvltrieeAllocator, AvltrieeNode};
use file_mmap::FileMmap;

pub struct IdxFileAvltrieeAllocator<T> {
    mmap: FileMmap,
    allocation_lot: u32,
    rows_capacity: u32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> IdxFileAvltrieeAllocator<T> {
    const UNIT_SIZE: u64 = size_of::<AvltrieeNode<T>>() as u64;

    pub fn new<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        let mut mmap = FileMmap::new(path).unwrap();
        if mmap.len() == 0 {
            mmap.set_len(Self::UNIT_SIZE).unwrap();
        }
        let rows_capacity = (mmap.len() / Self::UNIT_SIZE) as u32 - 1;
        IdxFileAvltrieeAllocator {
            mmap,
            allocation_lot,
            rows_capacity,
            _marker: PhantomData,
        }
    }
}

impl<T> AvltrieeAllocator<T> for IdxFileAvltrieeAllocator<T> {
    fn as_ptr(&self) -> *const AvltrieeNode<T> {
        self.mmap.as_ptr() as *const AvltrieeNode<T>
    }

    fn as_mut_ptr(&mut self) -> *mut AvltrieeNode<T> {
        self.mmap.as_mut_ptr() as *mut AvltrieeNode<T>
    }

    fn resize(&mut self, new_capacity: u32)
    where
        T: Clone + Default,
    {
        if self.rows_capacity < new_capacity {
            self.rows_capacity = (new_capacity / self.allocation_lot + 1) * self.allocation_lot;
            self.mmap
                .set_len(Self::UNIT_SIZE * (self.rows_capacity + 1) as u64)
                .unwrap();
        }
    }

    fn get(&self, row: std::num::NonZeroU32) -> Option<&AvltrieeNode<T>> {
        Some(unsafe { &*self.as_ptr().offset(row.get() as isize) })
    }

}
