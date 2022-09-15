use std::mem;
use std::cmp::Ordering;

use file_mmap::FileMmap;
use avltriee::{AVLTriee,AVLTrieeNode};

pub use avltriee::RemoveResult;
pub use avltriee::IdSet;

pub struct IndexedDataFile<T>{
    mmap:FileMmap
    ,triee:AVLTriee<T>
}

impl<T: std::default::Default + Copy> IndexedDataFile<T>{
    pub fn new(path:&str) -> Result<IndexedDataFile<T>,std::io::Error>{
        let init_size=mem::size_of::<usize>() as u64;   //ファイルの先頭にはtrieeのrootのポインタが入る。
                                                        //但し、アライメントを考慮して33bit-ワード長まではパディングされるようにusizeで計算しておく
        let mut filemmap=FileMmap::new(path,init_size)?;

        let p=filemmap.as_ptr() as *mut u32;

        let len=filemmap.len();
        let record_count=if len==init_size{
            0
        }else{
            (len-init_size)/mem::size_of::<AVLTrieeNode<T>>() as u64 - 1
        };
        let ep=unsafe{
            filemmap.as_mut_ptr().offset(init_size as isize)
        } as *mut AVLTrieeNode<T>;
        Ok(IndexedDataFile{
            mmap:filemmap
            ,triee:AVLTriee::new(
                p,ep,record_count as u32
            )
        })
    }
    pub fn triee_mut(&mut self)->&mut AVLTriee<T>{
        &mut self.triee
    }
    pub fn triee(&self)->&AVLTriee<T>{
        &self.triee
    }
    pub fn insert(&mut self,target:T)->Option<u32> where T:Default + std::cmp::Ord{
        if self.triee.record_count()==0{ //データがまだ無い場合は新規登録
            self.init(target)
        }else{
            let (ord,found_id)=self.triee.search(&target);
            assert_ne!(0,found_id);
            if ord==Ordering::Equal{
                self.insert_same(found_id)
            }else{
                self.insert_unique(target,found_id,ord)
            }
        }
    }
    pub fn update(&mut self,id:u32,value:T) where T:std::cmp::Ord{
        self.triee.update(id,value);
    }
    pub fn delete(&mut self,id:u32)->RemoveResult<T>{
        self.triee.remove(id)
    }
    pub fn resize_to(&mut self,record_count:u32)->Result<u32,std::io::Error>{
        let size=mem::size_of::<usize>()
            +mem::size_of::<AVLTrieeNode<T>>()*(1+record_count as usize)
        ;
        if self.mmap.len()<size as u64{
            self.triee.set_record_count(record_count);
            self.mmap.set_len(size as u64)?;
        }
        Ok(record_count)
    }
    fn resize(&mut self)->Result<u32,std::io::Error>{
        self.triee.add_record_count(1);
        let size=mem::size_of::<usize>()
            +mem::size_of::<AVLTrieeNode<T>>()*(1+self.triee.record_count() as usize)
        ;
        self.mmap.set_len(size as u64)?;
        Ok(self.triee.record_count())
    }
    pub fn init(&mut self,data:T)->Option<u32>{
        if let Err(_)=self.mmap.set_len((
            mem::size_of::<usize>()+mem::size_of::<AVLTrieeNode<T>>()*2
        ) as u64){
            None
        }else{
            self.triee.init_node(data);
            Some(1)
        }
    }
    pub fn insert_unique(&mut self
        ,data:T
        ,root: u32   //起点ノード（親ノード）
        ,ord: Ordering
    )->Option<u32> where T:Default{
        if root==0{    //初回登録
            self.init(data)
        }else{
            match self.resize(){
                Err(_)=>None
                ,Ok(new_id)=>{
                    self.triee.update_node(root,new_id,data,ord);
                    Some(new_id)
                 }
             }
         }
    }
    pub fn insert_same(&mut self,root:u32)->Option<u32>{
        match self.resize(){
            Err(_)=>None
            ,Ok(new_id)=>{
                self.triee.update_same(root,new_id);
                Some(new_id)
            }
        }
    }
}
