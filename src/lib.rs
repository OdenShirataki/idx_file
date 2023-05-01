use std::{cmp::Ordering, io, mem::size_of, path::Path};

pub use anyhow;
use anyhow::Result;

use avltriee::AvltrieeNode;
use avltriee::Removed as AvltrieeRemoved;
pub use avltriee::{Avltriee, AvltrieeIter, Found};
use file_mmap::FileMmap;

pub enum Removed<T> {
    Last(T),
    Remain,
    None,
}

pub struct IdxSized<T> {
    mmap: FileMmap,
    triee: Avltriee<T>,
}
impl<T> IdxSized<T> {
    const UNIT_SIZE: u64 = size_of::<AvltrieeNode<T>>() as u64;
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(Self::UNIT_SIZE)?;
        }
        let p = filemmap.as_ptr() as *mut AvltrieeNode<T>;
        Ok(IdxSized {
            mmap: filemmap,
            triee: Avltriee::new(p),
        })
    }
    pub fn triee(&self) -> &Avltriee<T> {
        &self.triee
    }
    pub fn value(&self, row: u32) -> Option<&T>
    {
        if let Ok(max_rows) = self.max_rows() {
            if row <= max_rows {
                return unsafe { self.triee.value(row) };
            }
        }
        None
    }

    pub fn insert(&mut self, value: T) -> io::Result<u32>
    where
        T: Clone + Ord,
    {
        if self.triee.root() == 0 {
            self.init(1, value)
        } else {
            let found = self.triee.search(&value);
            if found.ord() == Ordering::Equal {
                self.insert_same(found.row())
            } else {
                self.insert_unique(value, found)
            }
        }
    }
    pub fn insert_same(&mut self, row: u32) -> io::Result<u32>
    where
        T: Clone,
    {
        self.update_same(0, row)
    }
    pub fn insert_unique(&mut self, value: T, found: Found) -> io::Result<u32> {
        self.update_unique(0, value, found)
    }

    pub fn update(&mut self, row: u32, value: T) -> io::Result<u32>
    where
        T: Ord + Clone,
    {
        self.expand_to(row)?;
        unsafe {
            self.triee.update(row, value);
        }
        Ok(row)
    }
    pub fn update_manually<V>(&mut self, row: u32, mut make_value: V, found: Found) -> Result<u32>
    where
        V: FnMut() -> Result<T>,
        T: Clone,
    {
        let found_ord = found.ord();
        let found_row = found.row();
        if found_ord == Ordering::Equal && found_row != 0 {
            Ok(self.update_same(row, found_row)?)
        } else {
            let v = make_value()?;
            if self.exists(row) {
                unsafe {
                    self.triee.update_unique(row, v, found);
                }
                Ok(row)
            } else {
                Ok(self.update_unique(row, v, found)?)
            }
        }
    }

    pub fn delete(&mut self, row: u32) -> io::Result<Removed<T>>
    where
        T: Clone,
    {
        if let Ok(max_rows) = self.max_rows() {
            if row <= max_rows {
                let ret = {
                    match unsafe { self.triee.remove(row) } {
                        AvltrieeRemoved::Last => {
                            Removed::Last(unsafe { self.triee.value_unchecked(row) }.clone())
                        }
                        AvltrieeRemoved::Remain => Removed::Remain,
                        AvltrieeRemoved::None => Removed::None,
                    }
                };
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
                    self.mmap.set_len(Self::UNIT_SIZE * (current + 1) as u64)?;
                }
                return Ok(ret);
            }
        }
        Ok(Removed::None)
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

    fn update_unique(&mut self, row: u32, value: T, found: Found) -> io::Result<u32> {
        let parent = found.row();
        if parent == 0 {
            self.init(if row == 0 { 1 } else { row }, value)
        } else {
            let new_row = self.new_row(row)?;
            unsafe {
                self.triee.update_unique(new_row, value, found);
            }
            Ok(new_row)
        }
    }
    fn update_same(&mut self, row: u32, parent: u32) -> io::Result<u32>
    where
        T: Clone,
    {
        let new_row = self.new_row(row)?;
        unsafe {
            self.triee.update_same(new_row, parent);
        }
        Ok(new_row)
    }

    fn new_row(&mut self, row: u32) -> io::Result<u32> {
        let sizing_count = if row != 0 { row } else { self.max_rows()? + 1 };
        self.expand_to(sizing_count)
    }

    fn expand_to(&mut self, record_count: u32) -> io::Result<u32> {
        let size = Self::UNIT_SIZE * (record_count + 1) as u64;
        if self.mmap.len()? < size {
            self.mmap.set_len(size)?;
            self.triee = Avltriee::new(self.mmap.as_ptr() as *mut AvltrieeNode<T>);
        }
        Ok(record_count)
    }

    fn max_rows(&self) -> io::Result<u32> {
        Ok((self.mmap.len()? / Self::UNIT_SIZE) as u32 - 1)
    }
    fn init(&mut self, root: u32, data: T) -> io::Result<u32> {
        self.expand_to(root)?;
        self.triee.init_node(data, root);
        Ok(root)
    }
}

/*
fn example() {
    let mut idx = IdxSized::<i64>::new("example.idx").unwrap();
    idx.insert(100).unwrap();
    idx.insert(300).unwrap();
    idx.insert(100).unwrap();
    idx.insert(150).unwrap();

    idx.update(2, 50).unwrap();

    idx.delete(1).unwrap();

    for i in idx.triee().iter() {
        println!("{}. {} : {}", i.index(), i.row(), i.value());
    }

    for row in idx.triee().iter_by_value(&100) {
        println!("{}. {} : {}", row.index(),row.row(), row.value());
    }
    for row in idx.triee().iter_by_value_from(&100) {
        println!("{}. {} : {}", row.index(),row.row(), row.value());
    }
    for row in idx.triee().iter_by_value_to(&200) {
        println!("{}. {} : {}", row.index(),row.row(), row.value());
    }
    for row in idx.triee().iter_by_value_from_to(&100, &200) {
        println!("{}. {} : {}", row.index(),row.row(), row.value());
    }
}
*/
