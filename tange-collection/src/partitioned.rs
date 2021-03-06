extern crate tange;

use std::any::Any;
use std::hash::{Hasher,Hash};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::sync::Arc;

use tange::deferred::{Deferred, batch_apply, tree_reduce};
use interfaces::*;

pub fn block_reduce<
    A,
    B,
    Col: Any + Sync + Send + Clone + Stream<A>,
    K: Any + Sync + Send + Clone + Hash + Eq,
    C: Any + Sync + Send + Clone,
    D: 'static + Sync + Send + Clone + Fn() -> B, 
    F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
    O: 'static + Sync + Send + Clone + Fn(&mut B, &A) -> (),
    M: 'static + Sync + Send + Clone + Fn(HashMap<K,B>) -> C,
>(
    defs: &[Deferred<Col>], 
    key: F, 
    default: D, 
    binop: O,
    map: M
) -> Vec<Deferred<C>> {
    batch_apply(defs, move |_idx, vs| {
        let mut reducer = HashMap::new();
        for v in vs.stream().into_iter() {
            let k = key(&v);
            let e = reducer.entry(k).or_insert_with(&default);
            binop(e, &v);
        }
        map(reducer)
    })
}

pub fn split_by_key<
    Col: Any + Sync + Send + Clone + Accumulator<A> + Stream<A>,
    A: Clone,
    F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
>(
    defs: &[Deferred<Col>], 
    partitions: usize, 
    hash_function: F
) -> Vec<Vec<Deferred<Col>>> 
        where Col::VW: ValueWriter<A,Out=Col> {

    // Group into buckets 
    let stage1 = batch_apply(&defs, move |_idx, vs| {
        let mut parts: Vec<_> = (0..partitions).map(|_| vs.writer()).collect();
        for (idx, x) in vs.stream().into_iter().enumerate() {
            let p = hash_function(idx, &x) % partitions;
            parts[p].add(x.clone());
        }
        parts.into_iter().map(|x| x.finish()).collect::<Vec<_>>()
    });

    // For each partition in each chunk, pull out at index and regroup.
    // Tree reduce to concatenate
    let mut splits = Vec::with_capacity(partitions);
    for idx in 0usize..partitions {
        let mut partition = Vec::with_capacity(stage1.len());

        for s in stage1.iter() {
            partition.push(s.apply(move |parts| parts[idx].copy()));
        }
        splits.push(partition);
    }
    splits
}

pub fn partition<
    Col: Any + Sync + Send + Clone + Accumulator<A> + Stream<A>,
    A: Any + Send + Sync + Clone,
    F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
>(
    defs: &[Deferred<Col>], 
    partitions: usize, 
    key: F
) -> Vec<Deferred<Col>>
        where Col::VW: ValueWriter<A,Out=Col> {
    
    let groups = split_by_key(defs, partitions, key);
    
    let mut new_chunks = Vec::with_capacity(groups.len());
    for group in groups {
        if let Some(d) = concat(&group) {
            new_chunks.push(d);
        }
    }
    new_chunks
}

pub fn fold_by<
    A: Clone,
    C1: Any + Sync + Send + Clone + Accumulator<A> + Stream<A>,
    B: Any + Sync + Send + Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    D: 'static + Sync + Send + Clone + Fn() -> B, 
    F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
    O: 'static + Sync + Send + Clone + Fn(&mut B, &A) -> (),
    R: 'static + Sync + Send + Clone + Fn(&mut B, &B) -> (),
    Acc: 'static + Accumulator<(K, B)> + Stream<(K,B)>
>(
    defs: &[Deferred<C1>],
    key: F, 
    default: D, 
    binop: O, 
    reduce: R, 
    acc: Acc,
    partitions: usize
) -> Vec<Deferred<<<Acc as Accumulator<(K, B)>>::VW as ValueWriter<(K, B)>>::Out>>
        where Acc::VW: ValueWriter<(K, B),Out=Acc> {

    let acc2 = Arc::new(acc);
    let am = acc2.clone();
    let stage1 = block_reduce(defs, key, default, binop, move |x| {
        let mut out = am.writer();
        out.extend(&mut x.into_iter());
        out.finish()
    });

    // Split into chunks
    let chunks = partition_by_key::<Acc,_,_,_>(&stage1, partitions, |x| x.0.clone());

    // partition reduce
    let am = acc2.clone();
    let concat: Vec<_> = chunks.into_iter().map(move |chunk| {
        let am = am.clone();
        batch_apply(&chunk, move |_idx, vs| {
            let mut hm = HashMap::new();
            for (k, v) in vs.stream() {
                hm.insert(k, v);
            }
            let mut out = am.writer();
            out.extend(&mut hm.into_iter());
            out.finish()
        })
    }).collect();

    let mut reduction = Vec::new();
    let rm = Arc::new(reduce);
    for group in concat {
        let amc = acc2.clone();
        let ri = rm.clone();

        let out = tree_reduce(&group, move |left, right| {
            let mut nl = HashMap::new();
            for (k, v) in left.stream() {
                nl.insert(k, v);
            }
            for (k, v) in right.stream() {
                if !nl.contains_key(&k) {
                    nl.insert(k, v);
                } else {
                    nl.entry(k)
                        .and_modify(|e| ri(e, &v))
                        .or_insert_with(|| v); 
                }
            }
            let mut out = amc.writer();

            for item in nl.into_iter() {
                out.add(item);
            }
            out.finish()
        });
        reduction.push(out.unwrap());
    }
    reduction
}

pub fn partition_by_key<
    C: Any + Sync + Send + Clone + Accumulator<A> + Stream<A>,
    A: Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    F: 'static + Sync + Send + Clone + Fn(&A) -> K
>(
    defs: &[Deferred<C>], 
    n_chunks: usize, 
    key: F
) -> Vec<Vec<Deferred<C>>>
        where C::VW: ValueWriter<A,Out=C> {
    split_by_key(defs, n_chunks, move |_idx, v| {
        let k = key(v);
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    })
}

pub fn concat<
    Col: Any + Sync + Send + Accumulator<A> + Stream<A>,
    A: Clone,
>(
    defs: &[Deferred<Col>]
) -> Option<Deferred<Col>>
        where  Col::VW: ValueWriter<A,Out=Col> {

    tree_reduce(&defs, |x, y| {
        let mut out = x.writer();
        for xi in x.stream() {
            out.add(xi);
        }
        for yi in y.stream() {
            out.add(yi);
        }
        out.finish()
    })
}

pub fn join_on_key<
    A, 
    B,
    Col1: Any + Sync + Send + Clone + Stream<(K, A)>,
    Col2: Any + Sync + Send + Clone + Stream<(K, B)>,
    K: Any + Send + Sync + Clone + Hash + Eq,
    C: Any + Sync + Send + Clone,
    J: 'static + Sync + Send + Clone + Fn(&A, &B) -> C,
    Acc: 'static + Accumulator<(K, C)>
>(
    d1: &Deferred<Col1>, 
    d2: &Deferred<Col2>, 
    acc: Acc,
    joiner: J
) -> Deferred<<<Acc as Accumulator<(K, C)>>::VW as ValueWriter<(K, C)>>::Out> {

    d1.join(d2, move |left, right| {
        // Slurp up left into a hashmap
        let mut hm = HashMap::new();
        for (k, lv) in left.stream() {
            let e = hm.entry(k).or_insert_with(|| Vec::with_capacity(1)); 
            e.push(lv);
        }
        let mut ret = acc.writer();
        for (k, rv) in right.stream() {
            if let Some(lvs) = hm.get(&k) {
                for lv in lvs.iter() {
                    ret.add((k.clone(), joiner(&lv, &rv)))
                }
            }
        }
        ret.finish()
    })
}

