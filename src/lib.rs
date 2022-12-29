use avltriee::AvltrieeNode;
pub use avltriee::{Avltriee, AvltrieeIter, Removed};
use file_mmap::FileMmap;
use std::{cmp::Ordering, collections::BTreeSet, io, mem::size_of, path::Path};

pub type RowSet = BTreeSet<u32>;

pub struct IdxSized<T> {
    mmap: FileMmap,
    triee: Avltriee<T>,
}

const INIT_SIZE: u64 = size_of::<usize>() as u64;
impl<T> IdxSized<T> {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(INIT_SIZE)?;
        }
        let ep = unsafe { filemmap.offset(INIT_SIZE as isize) } as *mut AvltrieeNode<T>;
        let p = filemmap.as_ptr() as *mut u32;
        Ok(IdxSized {
            mmap: filemmap,
            triee: Avltriee::new(p, ep),
        })
    }
    pub fn triee_mut(&mut self) -> &mut Avltriee<T> {
        &mut self.triee
    }
    pub fn triee(&self) -> &Avltriee<T> {
        &self.triee
    }
    pub fn value(&self, row: u32) -> Option<T>
    where
        T: Clone,
    {
        if let Ok(max_rows) = self.max_rows() {
            if max_rows > row {
                return unsafe { self.triee.value(row) }.map(|v| v.clone());
            }
        }
        None
    }
    pub fn insert(&mut self, target: T) -> io::Result<u32>
    where
        T: Default + Clone + Ord,
    {
        if self.triee.root() == 0 {
            //データがまだ無い場合は新規登録
            self.init(target, 1)
        } else {
            let (ord, found_row) = self.triee.search(&target);
            assert_ne!(0, found_row);
            if ord == Ordering::Equal {
                self.insert_same(found_row, 0)
            } else {
                self.insert_unique(target, found_row, ord, 0)
            }
        }
    }
    pub fn update(&mut self, row: u32, value: T) -> io::Result<u32>
    where
        T: Ord + Clone + Default,
    {
        self.resize_to(row)?;
        unsafe {
            self.triee.update(row, value);
        }
        Ok(row)
    }
    pub fn delete(&mut self, row: u32) -> Removed<T>
    where
        T: Default + Clone,
    {
        if let Ok(max_rows) = self.max_rows() {
            if max_rows > row {
                return unsafe { self.triee.remove(row) };
            }
        }
        Removed::None
    }
    pub fn resize_to(&mut self, record_count: u32) -> io::Result<u32> {
        let size = INIT_SIZE + size_of::<AvltrieeNode<T>>() as u64 * (1 + record_count as u64);
        if self.mmap.len()? < size {
            self.mmap.set_len(size)?;
        }
        Ok(record_count)
    }
    pub fn max_rows(&self) -> io::Result<u32> {
        let len = self.mmap.len()?;
        Ok(((len - INIT_SIZE) / size_of::<AvltrieeNode<T>>() as u64) as u32)
    }
    fn get_to_new_row(&mut self, insert_row: u32) -> io::Result<u32> {
        let sizing_count = if insert_row != 0 {
            insert_row
        } else {
            self.max_rows()?
        };
        self.resize_to(sizing_count)
    }
    pub fn init(&mut self, data: T, root: u32) -> io::Result<u32>
    where
        T: Default,
    {
        self.mmap
            .set_len(INIT_SIZE + size_of::<AvltrieeNode<T>>() as u64 * (root + 1) as u64)?;
        self.triee.init_node(data, root);
        Ok(root)
    }
    pub fn insert_unique(
        &mut self,
        data: T,
        parent: u32, //起点ノード（親ノード）
        ord: Ordering,
        insert_row: u32,
    ) -> io::Result<u32>
    where
        T: Default,
    {
        if parent == 0 {
            //初回登録
            self.init(data, if insert_row == 0 { 1 } else { insert_row })
        } else {
            let new_row = self.get_to_new_row(insert_row)?;
            unsafe {
                self.triee.update_node(parent, new_row, data, ord);
            }
            Ok(new_row)
        }
    }
    pub fn insert_same(&mut self, parent: u32, insert_row: u32) -> io::Result<u32>
    where
        T: Clone,
    {
        let new_row = self.get_to_new_row(insert_row)?;
        unsafe {
            self.triee.update_same(parent, new_row);
        }
        Ok(new_row)
    }

    pub fn select_by_value(&self, value: &T) -> RowSet
    where
        T: Ord,
    {
        let mut result = RowSet::default();
        let (ord, row) = self.triee().search(value);
        if ord == Ordering::Equal && row > 0 {
            result.insert(row);
            result.append(
                &mut unsafe { self.triee().sames(row) }
                    .iter()
                    .map(|&x| x)
                    .collect(),
            );
        }
        result
    }
    pub fn select_by_value_from_to(&self, value_min: &T, value_max: &T) -> RowSet
    where
        T: Ord,
    {
        let mut result = RowSet::default();
        for r in self.triee().iter_by_value_from_to(value_min, value_max) {
            result.insert(r.row());
        }
        result
    }
    pub fn select_by_value_from(&self, value_min: &T) -> RowSet
    where
        T: Ord,
    {
        let mut result = RowSet::default();
        for r in self.triee().iter_by_value_from(value_min) {
            result.insert(r.row());
        }
        result
    }
    pub fn select_by_value_to(&self, value_max: &T) -> RowSet
    where
        T: Ord,
    {
        let mut result = RowSet::default();
        for r in self.triee().iter_by_value_to(value_max) {
            result.insert(r.row());
        }
        result
    }
}
