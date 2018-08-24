extern crate serde;
extern crate bincode;
extern crate uuid;

use std::any::Any;
use std::fs::{File,remove_file, copy};
use std::io::{BufReader,BufWriter};
use std::marker::PhantomData;

use self::serde::{Serialize,Deserialize};
use self::bincode::{serialize_into, deserialize_from};
use self::uuid::Uuid;

pub trait Accumulator<A>: Send + Sync + Clone  {
    type VW: ValueWriter<A>;
    
    fn writer(&self) -> Self::VW;

    fn write_vec(&self, vs: Vec<A>) -> <<Self as Accumulator<A>>::VW as ValueWriter<A>>::Out {
        let mut out = self.writer();
        for a in vs {
            out.add(a)
        }
        out.finish()
    }
}

pub trait ValueWriter<A>: Sized {
    type Out: Accumulator<A>;

    fn add(&mut self, item: A) -> ();

    fn extend<I: Iterator<Item=A>>(&mut self, i: &mut I) -> () {
        for item in i {
            self.add(item);
        }
    }

    fn finish(self) -> Self::Out;
}

#[derive(Clone)]
pub struct Memory;

impl <A: Any + Send + Sync + Clone> Accumulator<A> for Memory {
    type VW = Vec<A>;

    fn writer(&self) -> Self::VW {
        Vec::new()
    }
}

impl <A: Any + Send + Sync + Clone> Accumulator<A> for Vec<A> {
    type VW = Vec<A>;

    fn writer(&self) -> Self::VW {
        Vec::new()
    }
}

impl <A: Any + Send + Sync + Clone> ValueWriter<A> for Vec<A> {
    type Out = Vec<A>;

    fn add(&mut self, item: A) -> () {
        self.push(item);
    }

    fn finish(mut self) -> Self::Out {
        self.shrink_to_fit();
        self
    }
}

pub trait Stream<A> {
    type Iter: IntoIterator<Item=A>;

    fn stream(&self) -> Self::Iter;
    fn copy(&self) -> Self;
}

impl <A: Clone> Stream<A> for Vec<A> {
    type Iter = Vec<A>;

    fn stream(&self) -> Self::Iter {
        self.clone()
    }

    fn copy(&self) -> Self {
        self.clone()
    }
}

#[derive(Clone)]
pub struct Disk(pub String);

#[derive(Clone)]
pub struct DiskBuffer<A> {
    root_path: String, 
    buffer: Vec<A>
}

#[derive(Clone)]
pub struct FileStore<A: Clone + Send + Sync> {
    root_path: String, 
    name: Option<String>,
    pd: PhantomData<A>
}

impl <A: Clone + Send + Sync> FileStore<A> {
    pub fn empty(path: String) -> Self {
        FileStore {
            root_path: path,
            name: None,
            pd: PhantomData
        }
    }
}

impl <A: Clone + Send + Sync> Drop for FileStore<A> {
    fn drop(&mut self) {
        if let Some(ref name) = self.name {
            if let Err(e) = remove_file(name) {
                eprintln!("Error Deleting {}: {:?}J", name, e);
            }
        }
    }
}

impl <A: Serialize + Clone + Send + Sync> Accumulator<A> for Disk {
    type VW = DiskBuffer<A>;

    fn writer(&self) -> Self::VW {
        DiskBuffer { root_path: self.0.clone(), buffer: Vec::new() }
    }
}

impl <A: Serialize + Clone + Send + Sync> Accumulator<A> for FileStore<A> {
    type VW = DiskBuffer<A>;

    fn writer(&self) -> Self::VW {
        DiskBuffer { root_path: self.root_path.clone(), buffer: Vec::new() }
    }
}

impl <A: Serialize + Clone + Send + Sync> ValueWriter<A> for DiskBuffer<A> {
    type Out = FileStore<A>;

    fn add(&mut self, item: A) -> () {
        self.buffer.push(item);
    }

    fn finish(self) -> Self::Out {
        let name = format!("{}/tange-{}", &self.root_path, Uuid::new_v4());
        let fd = File::create(&name).expect("Can't create file!");
        let mut bw = BufWriter::new(fd);
        serialize_into(&mut bw, &self.buffer).expect("Couldn't write data!");
        FileStore { 
            root_path: self.root_path.clone(), 
            name: Some(name), 
            pd: PhantomData
        }
    }
}

impl <A: Clone + Send + Sync + for<'de> Deserialize<'de>> Stream<A> for FileStore<A> {
    type Iter = Vec<A>;

    fn stream(&self) -> Self::Iter {
        if let Some(ref name) = self.name {
            let fd = File::open(name).expect("File didn't exist on open!");
            let mut br = BufReader::new(fd);
            let v: Vec<A> = deserialize_from(&mut br).expect("Unable to deserialize item!");
            v
        } else {
            Vec::with_capacity(0)
        }
    }

    fn copy(&self) -> Self {
        if let Some(ref name) = self.name {
            let new_name = format!("{}/tange-{}", &self.root_path, Uuid::new_v4());
            copy(name, &new_name).expect("Failed to copy file!");
            FileStore {
                root_path: self.root_path.clone(),
                name: Some(new_name),
                pd: PhantomData
            }
        } else {
            FileStore::empty(self.root_path.clone())
        }
    }
}
