//! Defines the Deferred primitive
//!
use std::marker::PhantomData;
use std::sync::Arc;
use std::any::Any;

use task::{DynFn,DynFn2,BASS};
use graph::*;
use scheduler::Scheduler;

struct Lift<A>(A);

impl <A: Any + Send + Sync + Clone> Input for Lift<A> {
    fn read(&self) -> BASS {
        Box::new(self.0.clone())
    }
}

/// A `Deferred` is the core struct defining how computations are composed
/// The type parameter indicates the type of data contained within the `Deferred`
#[derive(Clone)]
pub struct Deferred<A> {

    /// Dependency graph required to evaluate to the given A
    graph: Arc<Graph>,

    /// Phantom type for Any
    items: PhantomData<A>
}

impl <A: Any + Send + Sync> Deferred<A> {
    
    /// Applies a function to a Deferred, returning a new Deferred.  This is effectively
    /// a Functor.
    ///
    /// ```
    /// use tange::deferred::Deferred;
    /// use tange::scheduler::GreedyScheduler;
    ///
    /// let def = Deferred::lift(vec![1u8, 2, 3, 4], "Vector".into());
    /// let size = def.apply(|v| v.len());
    /// let results = size.run(&GreedyScheduler::new());
    /// assert_eq!(results, Some(4usize));
    /// ```
    ///
    pub fn apply<B: Any + Send + Sync, F: Send + Sync + 'static + Fn(&A) -> B>(&self, f: F) -> Deferred<B> {
        let ng = Graph::create_task(
            FnArgs::Single(self.graph.clone()), DynFn::new(f), "Apply");
        Deferred {
            graph: ng,
            items: PhantomData
        }

    }

    /// Joins two Deferred objects with a function, creating a new Deferred object.
    ///
    /// ```
    /// use tange::deferred::Deferred;
    /// use tange::scheduler::GreedyScheduler;
    ///
    /// let left  = Deferred::lift(vec![1f32, 2., 3., 4.], "Vector".into());
    /// let right  = Deferred::lift(10f32, "Num".into());
    /// let multiplied: Deferred<Vec<f32>> = left.join(&right, 
    ///        |l,r| l.iter().map(|x| x * r).collect());
    /// let results = multiplied.run(&GreedyScheduler::new());
    /// assert_eq!(results, Some(vec![10., 20., 30., 40.]));
    /// ```
    ///
    pub fn join<B: Any + Send + Sync, C: Any + Send + Sync, F: Send + Sync + 'static + Fn(&A, &B) -> C>(&self, other: &Deferred<B>, f: F) -> Deferred<C> {
        let ng = Graph::create_task(
            FnArgs::Join(self.graph.clone(), other.graph.clone()), 
            DynFn2::new(f), "Join");

        Deferred {
            graph: ng,
            items: PhantomData
        }

    }
}

impl <A: Any + Send + Sync + Clone> Deferred<A> {
    /// Lifts a value into a Deferred object.
    /// ```
    /// use tange::deferred::Deferred;
    /// use tange::scheduler::GreedyScheduler;
    ///
    /// let id = Deferred::lift("Some String".to_owned(), "String".into());
    /// assert_eq!(id.run(&GreedyScheduler::new()), Some("Some String".into()));
    /// 
    /// ```
    pub fn lift(a: A, name: Option<&str>) -> Self {
        let graph = Graph::create_input(Lift(a), name.unwrap_or("Input"));
        Deferred {
            graph: graph,
            items: PhantomData
        }
    }

    /// Evaluates the Deferred object and dependency graph, returning the result 
    /// of the computation.  
    /// 
    /// ```
    /// use tange::deferred::Deferred;
    /// use tange::scheduler::GreedyScheduler;
    ///
    /// let a = Deferred::lift(1usize, "a".into());
    /// let b = Deferred::lift(2usize, "b".into());
    /// let c = a.join(&b, |x, y| x + y);
    /// assert_eq!(c.run(&GreedyScheduler::new()), Some(3usize));
    /// 
    /// ```

    pub fn run<S: Scheduler>(&self, s: &S) -> Option<A> {
        s.compute(self.graph.clone()).and_then(|v| { 
            Arc::try_unwrap(v).ok().and_then(|ab| {
                ab.downcast_ref::<A>().map(|x| x.clone())
            })
        })
    }
}

/// `batch_apply` is a convenience method that takes a set of homogenous `Deferred`s
/// and applies a function to each, returning a new set of `Deferred`s.  Unlike 
/// `Deferred::apply`, `batch_apply` passes in an order index. 
/// ```
/// use tange::deferred::{Deferred, batch_apply};
/// use tange::scheduler::GreedyScheduler;
///
/// let vec: Vec<_> = (0usize..10)
///     .map(|v| Deferred::lift(v, None)).collect();
/// let out = batch_apply(&vec, |idx, v| idx + v);
/// assert_eq!(out[1].run(&GreedyScheduler::new()), Some(2));
/// assert_eq!(out[5].run(&GreedyScheduler::new()), Some(10));
/// ```
///
pub fn batch_apply<
    A: Any + Send + Sync + Clone, 
    B: Any + Send + Sync, 
    F: 'static + Sync + Send + Clone + Fn(usize, &A) -> B
    >(defs: &[Deferred<A>], f: F) 
-> Vec<Deferred<B>> {
    let mut nps = Vec::with_capacity(defs.len());
    let fa = Arc::new(f);
    for (idx, p) in defs.iter().enumerate() {
        let mf = fa.clone();
        let np = p.apply(move |vs| { mf(idx, vs) }); 
        nps.push(np);
    }   
    nps 
}

/// Often times, we want to combine a set of Deferred objects into a single Deferred.
/// `tree_reduce` combines pairs of Deferred recursively using `f`, building a dependency
/// tree which attempts to maximize parallelism.
/// ```
/// use tange::deferred::{Deferred, tree_reduce};
/// use tange::scheduler::GreedyScheduler;
///
/// let vec: Vec<_> = (0usize..10)
///     .map(|v| Deferred::lift(v, None)).collect();
/// let out = tree_reduce(&vec, |left, right| left + right).unwrap();
/// let expected = (0usize..10).fold(0, |acc, x| acc + x);
/// assert_eq!(out.run(&GreedyScheduler::new()), Some(expected));
/// ```
pub fn tree_reduce<A: Any + Send + Sync + Clone, 
                   F: 'static + Sync + Send + Clone + Fn(&A, &A) -> A
>(
    defs: &[Deferred<A>], 
    f: F
) -> Option<Deferred<A>> {
    tree_reduce_until(defs, 1, f).map(|mut defs| {
        defs.remove(0)
    })
}

/// `tree_reduce_until` is similar to `tree_reduce` except that it will stop reducing
/// when the number of `Deferred`s left is less than or equal to `parts`.
///
/// ```
/// use tange::deferred::{Deferred, tree_reduce_until};
/// use tange::scheduler::GreedyScheduler;
///
/// let vec: Vec<_> = (0usize..8)
///     .map(|v| Deferred::lift(v, None)).collect();
/// let out = tree_reduce_until(&vec, 2, |left, right| left + right).unwrap();
/// assert_eq!(out.len(), 2);
/// assert_eq!(out[0].run(&GreedyScheduler::new()), Some(0+1+2+3));
/// ```
pub fn tree_reduce_until<A: Any + Send + Sync + Clone, 
                   F: 'static + Sync + Send + Clone + Fn(&A, &A) -> A
>(
    defs: &[Deferred<A>], 
    parts: usize, 
    f: F
) -> Option<Vec<Deferred<A>>> {
    if defs.len() == 0 {
        None
    } else if defs.len() <= parts {
        Some(defs.clone().to_vec())
    } else {
        // First pass
        let mut pass = Vec::new();
        for i in (0..defs.len() - 1).step_by(2) {
            pass.push(defs[i].join(&defs[i+1], f.clone()));
        }
        if defs.len() % 2 == 1 {
            pass.push(defs[defs.len() - 1].clone());
        }
        tree_reduce_until(&pass, parts, f)
    }
}

#[cfg(test)]
mod def_test {
    use super::*;
    use scheduler::{LeveledScheduler,GreedyScheduler};

    #[test]
    fn test_tree_reduce() {
        let v: Vec<_> = (0..999usize).into_iter()
            .map(|x| Deferred::lift(x, None))
            .collect();

        let res = (0..999usize).sum();

        let agg = tree_reduce(&v, |x, y| x + y).unwrap();
        let results = agg.run(&LeveledScheduler);
        assert_eq!(results, Some(res));
    }

    #[test]
    fn test_tree_reduce_greedy() {
        let v: Vec<_> = (0..2usize).into_iter()
            .map(|x| Deferred::lift(x, None))
            .collect();

        let res = (0..2usize).sum();

        let agg = tree_reduce(&v, |x, y| x + y).unwrap();
        let results = agg.run(&GreedyScheduler::new());
        assert_eq!(results, Some(res));
    }

}
