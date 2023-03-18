use avltriee::AvltrieeNode;
pub use avltriee::{Avltriee, AvltrieeIter, Removed};
use file_mmap::FileMmap;
use std::{cmp::Ordering, collections::BTreeSet, io, mem::size_of, path::Path};

pub type RowSet = BTreeSet<u32>;

pub struct IdxSized<T> {
    mmap: FileMmap,
    triee: Avltriee<T>,
}

const ROOT_SIZE: u64 = size_of::<usize>() as u64;
impl<T> IdxSized<T> {
    const UNIT_SIZE: u64 = size_of::<AvltrieeNode<T>>() as u64;
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(ROOT_SIZE + Self::UNIT_SIZE)?;
        }
        let ep = unsafe { filemmap.offset(ROOT_SIZE as isize) } as *mut AvltrieeNode<T>;
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
            if row <= max_rows {
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
        self.expand_to(row)?;
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
            if row <= max_rows {
                let ret = unsafe { self.triee.remove(row) };
                if row == max_rows {
                    let mut current = row;
                    while let None = self.value(current - 1) {
                        current -= 1;
                        if current == 0 {
                            break;
                        }
                    }
                    self.mmap
                        .set_len(ROOT_SIZE + Self::UNIT_SIZE * current as u64)
                        .unwrap();
                }
                return ret;
            }
        }
        Removed::None
    }
    fn expand_to(&mut self, record_count: u32) -> io::Result<u32> {
        let size = ROOT_SIZE + Self::UNIT_SIZE * (record_count + 1) as u64;
        if self.mmap.len()? < size {
            self.mmap.set_len(size)?;
            self.triee = Avltriee::new(self.mmap.as_ptr() as *mut u32, unsafe {
                self.mmap.offset(ROOT_SIZE as isize) as *mut AvltrieeNode<T>
            });
        }
        Ok(record_count)
    }
    pub fn max_rows(&self) -> io::Result<u32> {
        Ok(((self.mmap.len()? - ROOT_SIZE) / Self::UNIT_SIZE) as u32 - 1)
    }
    fn new_row(&mut self, insert_row: u32) -> io::Result<u32> {
        let sizing_count = if insert_row != 0 {
            insert_row
        } else {
            self.max_rows()? + 1
        };
        self.expand_to(sizing_count)
    }
    pub fn init(&mut self, data: T, root: u32) -> io::Result<u32>
    where
        T: Default,
    {
        self.expand_to(root)?;
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
            let new_row = self.new_row(insert_row)?;
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
        let new_row = self.new_row(insert_row)?;
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
