use std::mem;
use std::cmp::Ordering;

use file_mmap::*;
use tri_avltree::{TriAVLTree,node::TriAVLTreeNode};

pub struct IndexedDataFile<T>{
    mmap:FileMmap
    ,tree:TriAVLTree<T>
}

impl<T: std::default::Default + std::fmt::Debug + Copy> IndexedDataFile<T>{
    pub fn new(path:&str) -> Result<IndexedDataFile<T>,std::io::Error>{
        let init_size=mem::size_of::<i64>() as u64;
        let mut filemmap=FileMmap::new(path,init_size)?;

        let p=filemmap.as_ptr() as *mut i64;

        let len=filemmap.len();
        let record_count=if len==init_size{
            0
        }else{
            (len-init_size)/mem::size_of::<TriAVLTreeNode<T>>() as u64 - 1
        };
        let ep=unsafe{
            filemmap.as_mut_ptr().offset(init_size as isize)
        } as *mut TriAVLTreeNode<T>;
        Ok(IndexedDataFile{
            mmap:filemmap
            ,tree:TriAVLTree::new(
                p,ep,record_count as usize
            )
        })
    }
    pub fn tree_mut(&mut self)->&mut TriAVLTree<T>{
        &mut self.tree
    }
    pub fn tree(&self)->&TriAVLTree<T>{
        &self.tree
    }
    pub fn insert(&mut self,target:T)->Option<i64> where T:Default + std::cmp::Ord{
        if self.tree.record_count()==0{ //データがまだ無い場合は新規登録
            self.init(target)
        }else{
            let (ord,found_id)=self.tree.search(&target);
            assert_ne!(0,found_id);
            if ord==Ordering::Equal{
                self.add_same(found_id,target)
            }else{
                self.add_new(target,found_id,ord)
            }
        }
    }
    pub fn resize_to(&mut self,record_count:i64)->Result<i64,std::io::Error>{
        let size=mem::size_of::<u64>()
            +mem::size_of::<TriAVLTreeNode<T>>()*(1+record_count as usize)
        ;
        if self.mmap.len()<size as u64{
            self.tree.set_record_count(record_count as usize);
            self.mmap.set_len(size as u64)?;
        }
        Ok(record_count)
    }
    fn resize(&mut self)->Result<u64,std::io::Error>{
        self.tree.add_record_count(1);
        let size=mem::size_of::<u64>()
            +mem::size_of::<TriAVLTreeNode<T>>()*(1+self.tree.record_count() as usize)
        ;
        self.mmap.set_len(size as u64)?;
        Ok(self.tree.record_count() as u64)
    }
    pub fn init(&mut self,data:T)->Option<i64>{
        if let Err(_)=self.mmap.set_len((
            mem::size_of::<u64>()+mem::size_of::<TriAVLTreeNode<T>>()*2
        ) as u64){
            None
        }else{
            self.tree.init_node(data);
            Some(1)
        }
    }
    pub fn add_new(&mut self
        ,data:T
        ,root: i64   //起点ノード（親ノード）
        ,ord: Ordering
    )->Option<i64> where T:Default{
        if root==0{    //初回登録
            self.init(data)
        }else{
            match self.resize(){
                Err(_)=>None
                ,Ok(newid)=>{
                    self.tree.update_node(root,newid as i64,data,ord);
                    Some(newid as i64)
                 }
             }
         }
    }
    pub fn add_same(&mut self,root:i64,data:T)->Option<i64>{
        match self.resize(){
            Err(_)=>None
            ,Ok(newid)=>{
                self.tree.update_same(root,newid as i64,data);
                Some(newid as i64)
            }
        }
    }
}
