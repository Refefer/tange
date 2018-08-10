extern crate tange;

pub mod utils;

use std::any::Any;
use std::hash::Hash;
use std::collections::HashMap;

use tange::deferred::{Deferred, batch_apply, tree_reduce};
use tange::scheduler::Scheduler;


#[derive(Clone)]
pub struct Collection<A>  {
    partitions: Vec<Deferred<Vec<A>>>
}

impl <A: Any + Send + Sync + Clone> Collection<A> {
    pub fn from_vec(vs: Vec<A>) -> Collection<A> {
        Collection {
            partitions: vec![Deferred::lift(vs, None)],
        }
    }

    pub fn n_partitions(&self) -> usize {
        self.partitions.len()
    }

    pub fn concat(&self, other: &Collection<A>) -> Collection<A> {
        let mut nps: Vec<Deferred<Vec<A>>> = self.partitions.iter()
            .map(|p| (*p).clone()).collect();
        for p in other.partitions.iter() {
            nps.push(p.clone());
        }
        Collection { partitions: nps }
    }
    
    pub fn map<B: Any + Send + Sync + Clone, F: 'static + Sync + Send + Clone + Fn(&A) -> B>(&self, f: F) -> Collection<B> {
        let out = batch_apply(&self.partitions, move |vs| {
            let mut agg = Vec::with_capacity(vs.len());
            for v in vs {
                agg.push(f(v));
            }
            agg
        });
        Collection { partitions: out }
    }

    pub fn filter<F: 'static + Sync + Send + Clone + Fn(&A) -> bool>(&self, f: F) -> Collection<A> {
        let out = batch_apply(&self.partitions, move |vs| {
            let mut agg = Vec::with_capacity(vs.len());
            for v in vs {
                if f(v) {
                    agg.push(v.clone());
                }
            }
            agg
        });
        Collection { partitions: out }
    }

    pub fn partition<F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize>(&self, partitions: usize, f: F) -> Collection<A> {
        // Group into buckets 
        let stage1 = batch_apply(&self.partitions, move |vs| {
            let mut parts = vec![Vec::new(); partitions];
            for (idx, x) in vs.iter().enumerate() {
                let p = f(idx, x) % partitions;
                parts[p].push(x.clone());
            }
            parts
        });

        // For each partition in each chunk, pull out at index and regroup.
        // Tree reduce to concatenate
        let mut new_chunks = Vec::with_capacity(partitions);
        for idx in 0usize..partitions {
            let mut partition = Vec::with_capacity(stage1.len());

            for s in stage1.iter() {
                partition.push(s.apply(move |parts| parts[idx].clone()));
            }

            let output = tree_reduce(&partition, |x, y| {
                let mut v1: Vec<_> = (*x).clone();
                for yi in y {
                    v1.push(yi.clone());
                }
                v1
            });
            if let Some(d) = output {
                new_chunks.push(d);
            }
        }
        // Loop over each bucket
        Collection { partitions: new_chunks }
    }

    pub fn fold_by<K: Any + Sync + Send + Clone + Hash + Eq,
                   B: Any + Sync + Send + Clone,
                   D: 'static + Sync + Send + Clone + Fn() -> B, 
                   F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
                   O: 'static + Sync + Send + Clone + Fn(&B, &A) -> B,
                   R: 'static + Sync + Send + Clone + Fn(&B, &B) -> B>(
        &self, key: F, default: D, binop: O, reduce: R
    ) -> Collection<(K,B)> {

        // First stage is to reduce each block internally
        let stage1 = batch_apply(&self.partitions, move |vs| {
            let mut reducer = HashMap::new();
            for v in vs {
                let k = key(v);
                let e = reducer.entry(k).or_insert_with(&default);
                *e = binop(e, v);
            }
            reducer
        });

        // Second, do block joins
        let reduction = tree_reduce(&stage1, move |left, right| {
            let mut nl: HashMap<_,_> = left.clone();
            for (k, v) in right {
                nl.entry(k.clone())
                    .and_modify(|e| *e = reduce(e, v))
                    .or_insert(v.clone()); 
            }
            nl
        });

        // Flatten
        let output = reduction.unwrap().apply(|vs| {
            vs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        });
        Collection { partitions: vec![output] }
    }

    pub fn run<S: Scheduler>(&self, s: &mut S) -> Option<Vec<A>> {
        let cat = tree_reduce(&self.partitions, |x, y| {
            let mut v1: Vec<_> = (*x).clone();
            for yi in y {
                v1.push(yi.clone());
            }
            v1
        });
        cat.and_then(|x| x.run(s))
    }
}

impl <A: Any + Send + Sync + Clone> Collection<Vec<A>> {
    pub fn flatten(&self) -> Collection<A> {
        let nps = batch_apply(&self.partitions, |vss| {
            let mut new_v = Vec::new();
            for vs in vss {
                for v in vs {
                    new_v.push(v.clone());
                }
            }
            new_v
        });

        Collection { partitions: nps }
    }
}

