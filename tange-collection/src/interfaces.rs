use std::any::Any;
use tange::deferred::Deferred;

pub trait Accumulator<A>: Send + Sync + Clone  {
    type VW: ValueWriter<A>;
    
    fn writer(&self) -> Self::VW;
}

pub trait ValueWriter<A>: Sized {
    type Out: Send + Sync + Any;

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

impl <A: Any + Send + Sync> Accumulator<A> for Memory {
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

impl <A: Any + Send + Sync> ValueWriter<A> for Vec<A> {
    type Out = Vec<A>;

    fn add(&mut self, item: A) -> () {
        self.push(item);
    }

    fn finish(self) -> Self::Out {
        self
    }
}

trait Partition: IntoIterator + Sized {
    fn partition<
        F: 'static + Sync + Send + Clone + Fn(usize, &Self::Item) -> usize
    >(
        defs: &[Deferred<Self>], 
        partitions: usize, 
        key: F
    ) -> Vec<Deferred<Self>>;
}
