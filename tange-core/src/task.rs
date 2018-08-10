use std::any::Any;
use std::marker::PhantomData;

pub type BASS = Box<Any + Send + Sync>;
pub enum DynArgs<'a> {
    One(&'a BASS),
    Two(&'a BASS, &'a BASS)
}

pub trait DynRun: Send + Sync {
    fn eval(&self, val: DynArgs) -> Option<BASS>;
}

pub struct DynFn<A,B,F: Fn(&A) -> B>(F,PhantomData<A>,PhantomData<B>);

impl <A,B,F: Fn(&A) -> B> DynFn<A,B,F> {
    pub fn new(f: F) -> Self {
        DynFn(f, PhantomData, PhantomData)
    }
}

impl <A: Any + Send + Sync, B: Any + Send + Sync, F: Send + Sync + Fn(&A) -> B> DynRun for DynFn<A,B,F> {

    fn eval(&self, val: DynArgs) -> Option<BASS> {
        match val {
            DynArgs::One(v) => v.downcast_ref::<A>().map(|a| {
                let b = self.0(a);
                let bx: BASS = Box::new(b);
                bx
            }),
            _ => None
        }
    }
}

pub struct DynFn2<A,B,C,F: Fn(&A, &B) -> C>(F,PhantomData<A>,PhantomData<B>,PhantomData<C>);

impl <A,B,C,F: Fn(&A, &B) -> C> DynFn2<A,B,C,F> {
    pub fn new(f: F) -> Self {
        DynFn2(f, PhantomData, PhantomData, PhantomData)
    }
}

impl <A: Any + Send + Sync, B: Any + Send + Sync, C: Any + Send + Sync, F: Send + Sync + Fn(&A, &B) -> C> DynRun for DynFn2<A,B,C,F> {

    fn eval(&self, val: DynArgs) -> Option<BASS> {
        match val {
            DynArgs::Two(a, b) => {
                a.downcast_ref::<A>().and_then(|a| {
                    b.downcast_ref::<B>().map(|b| {
                        let c = self.0(a, b);
                        let cx: BASS = Box::new(c);
                        cx
                    })
                })
            },
            _ => None
        }
    }
}


