use std::mem;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use file_mmap::FileMmap;
use avltriee::AvltrieeNode;
pub use avltriee::{
    Avltriee
    ,Removed
};

pub type RowSet = BTreeSet<u32>;

pub struct IdxSized<T>{
    mmap:FileMmap
    ,triee:Avltriee<T>
}

const INIT_SIZE: u64=mem::size_of::<usize>() as u64;
impl<T: std::default::Default + Copy> IdxSized<T>{
    pub fn new(path:&str) -> Result<IdxSized<T>,std::io::Error>{
        let filemmap=FileMmap::new(path,INIT_SIZE)?;
        let ep=filemmap.offset(INIT_SIZE as isize) as *mut AvltrieeNode<T>;
        let p=filemmap.as_ptr() as *mut u32;
        Ok(IdxSized{
            mmap:filemmap
            ,triee:Avltriee::new(
                p
                ,ep
            )
        })
    }
    pub fn triee_mut(&mut self)->&mut Avltriee<T>{
        &mut self.triee
    }
    pub fn triee(&self)->&Avltriee<T>{
        &self.triee
    }
    pub fn value(&self,row:u32)->Option<T>{
        self.triee.entity_value(row).map(|v|*v)
    }
    pub fn insert(&mut self,target:T)->Option<u32> where T:Default + std::cmp::Ord{
        if self.triee.root()==0{ //データがまだ無い場合は新規登録
            self.init(target,1)
        }else{
            let (ord,found_row)=self.triee.search(&target);
            assert_ne!(0,found_row);
            if ord==Ordering::Equal{
                self.insert_same(found_row,0)
            }else{
                self.insert_unique(target,found_row,ord,0)
            }
        }
    }
    pub fn update(&mut self,row:u32,value:T) where T:std::cmp::Ord{
        self.triee.update(row,value);
    }
    pub fn delete(&mut self,row:u32)->Removed<T>{
        self.triee.remove(row)
    }
    pub fn resize_to(&mut self,record_count:u32)->Result<u32,std::io::Error>{
        let size=mem::size_of::<usize>()
            +mem::size_of::<AvltrieeNode<T>>()*(1+record_count as usize)
        ;
        if self.mmap.len()<size as u64{
            self.mmap.set_len(size as u64)?;
        }
        Ok(record_count)
    }
    pub fn max_rows(&self)->u32{
        let len=self.mmap.len();
        
        ((len-INIT_SIZE)/mem::size_of::<AvltrieeNode<T>>() as u64) as u32
    }
    fn resize(&mut self,insert_row:u32)->Result<u32,std::io::Error>{
        let new_record_count=self.max_rows();
        let sizing_count=if insert_row!=0{
            insert_row
        }else{
            new_record_count
        };
        let size=mem::size_of::<usize>()
            +mem::size_of::<AvltrieeNode<T>>()*(1+sizing_count as usize)
        ;
        if (self.mmap.len() as usize)<size{
            self.mmap.set_len(size as u64)?;
        }
        
        Ok(sizing_count)
    }
    pub fn init(&mut self,data:T,root:u32)->Option<u32>{
        if let Err(_)=self.mmap.set_len((
            mem::size_of::<usize>()
            +mem::size_of::<AvltrieeNode<T>>()*(root as usize+1)
        ) as u64){
            None
        }else{
            self.triee.init_node(data,root);
            Some(root)
        }
    }
    pub fn insert_unique(&mut self
        ,data:T
        ,root: u32   //起点ノード（親ノード）
        ,ord: Ordering
        ,insert_row:u32
    )->Option<u32> where T:Default{
        if root==0{    //初回登録
            self.init(data,if insert_row==0{
                1
            }else{
                insert_row
            })
        }else{
            match self.resize(insert_row){
                Err(_)=>None
                ,Ok(new_row)=>{
                    self.triee.update_node(root,new_row,data,ord);
                    Some(new_row)
                 }
             }
         }
    }
    pub fn insert_same(&mut self,root:u32,insert_row:u32)->Option<u32>{
        match self.resize(insert_row){
            Err(_)=>None
            ,Ok(new_row)=>{
                self.triee.update_same(root,new_row);
                Some(new_row)
            }
        }
    }

    pub fn select_by_value(&self,value:&T)->RowSet where T:std::cmp::Ord{
        let mut result=RowSet::default();
        let (ord,row)=self.triee().search(value);
        if ord==Ordering::Equal{
            result.insert(row);
            result.append(&mut self.triee().sames(row).iter().map(|&x|x).collect());
        }
        result
    }
    pub fn select_by_value_from_to(&self,value_min:&T,value_max:&T)->RowSet where T:std::cmp::Ord{
        let mut result=RowSet::default();
        for (_,i,_) in self.triee().iter_by_value_from_to(value_min,value_max){
            result.insert(i);
        }
        result
    }
    pub fn select_by_value_from(&self,value_min:&T)->RowSet where T:std::cmp::Ord{
        let mut result=RowSet::default();
        for (_,i,_) in self.triee().iter_by_value_from(value_min){
            result.insert(i);
        }
        result
    }
    pub fn select_by_value_to(&self,value_max:&T)->RowSet where T:std::cmp::Ord{
        let mut result=RowSet::default();
        for (_,i,_) in self.triee().iter_by_value_to(value_max){
            result.insert(i);
        }
        result
    }
}
