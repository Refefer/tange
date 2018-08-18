extern crate tange;

use std::any::Any;
use std::hash::{Hasher,Hash};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;

use tange::deferred::{Deferred, batch_apply, tree_reduce};
use interfaces::*;

pub fn block_reduce<
    Col: Any + Sync + Send + Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    A,
    B,
    C: Any + Sync + Send + Clone,
    D: 'static + Sync + Send + Clone + Fn() -> B, 
    F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
    O: 'static + Sync + Send + Clone + Fn(&B, &A) -> B,
    M: 'static + Sync + Send + Clone + Fn(HashMap<K,B>) -> C,
>(
    defs: &[Deferred<Col>], 
    key: F, 
    default: D, 
    binop: O,
    map: M
) -> Vec<Deferred<C>> 
        where for<'a> &'a Col: IntoIterator<Item=&'a A> {
    batch_apply(defs, move |_idx, vs| {
        let mut reducer = HashMap::new();
        for v in vs.into_iter() {
            let k = key(v);
            let e = reducer.entry(k).or_insert_with(&default);
            *e = binop(e, v);
        }
        map(reducer)
    })
}

pub fn split_by_key<
    Col: Any + Sync + Send + Clone + Accumulator<A>,
    A: Clone,
    F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
>(
    defs: &[Deferred<Col>], 
    partitions: usize, 
    hash_function: F
) -> Vec<Vec<Deferred<Col>>> 
        where for<'a> &'a Col: IntoIterator<Item=&'a A>,
              Col::VW: ValueWriter<A,Out=Col> {

    // Group into buckets 
    let stage1 = batch_apply(&defs, move |_idx, vs| {
        let mut parts: Vec<_> = (0..partitions).map(|_| vs.writer()).collect();
        for (idx, x) in vs.into_iter().enumerate() {
            let p = hash_function(idx, x) % partitions;
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
            partition.push(s.apply(move |parts| parts[idx].clone()));
        }
        splits.push(partition);
    }
    splits
}

pub fn partition<
    Col: Any + Sync + Send + Clone + Accumulator<A>,
    A: Any + Send + Sync + Clone,
    F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
>(
    defs: &[Deferred<Col>], 
    partitions: usize, 
    key: F
) -> Vec<Deferred<Col>>
        where for<'a> &'a Col: IntoIterator<Item=&'a A>, 
              Col::VW: ValueWriter<A,Out=Col> {
    
    let groups = split_by_key(defs, partitions, key);
    
    let mut new_chunks = Vec::with_capacity(groups.len());
    for group in groups {
        if let Some(d) = concat(&group) {
            new_chunks.push(d);
        }
    }
    new_chunks
}

fn merge_maps<
    K: Hash + Eq + Clone, 
    V: Clone,
    R: 'static + Sync + Send + Clone + Fn(&V, &V) -> V
>(
    left: &HashMap<K, V>, 
    right: &HashMap<K,V>,
    reduce: R
) -> HashMap<K, V> {
    let mut nl: HashMap<_,_> = left.clone();
    for (k, v) in right.iter() {
        if !nl.contains_key(k) {
            nl.insert(k.clone(), v.clone());
        } else {
            nl.entry(k.clone())
                .and_modify(|e| *e = reduce(e, v))
                .or_insert_with(|| v.clone()); 
        }
    }
    nl
}

pub fn fold_by<
    C1: Any + Sync + Send + Clone + Accumulator<A>,
    A: Clone,
    B: Any + Sync + Send + Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    D: 'static + Sync + Send + Clone + Fn() -> B, 
    F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
    O: 'static + Sync + Send + Clone + Fn(&B, &A) -> B,
    R: 'static + Sync + Send + Clone + Fn(&B, &B) -> B,
    Acc: 'static + Accumulator<(K, B)>
>(
    defs: &[Deferred<C1>],
    key: F, 
    default: D, 
    binop: O, 
    reduce: R, 
    acc: Acc,
    partitions: usize
) -> Vec<Deferred<<<Acc as Accumulator<(K, B)>>::VW as ValueWriter<(K, B)>>::Out>>
        where for<'a> &'a C1: IntoIterator<Item=&'a A>,
              for<'a> &'a Acc: IntoIterator<Item=&'a (K,B)>,
              Acc::VW: ValueWriter<(K, B),Out=Acc> {

    let acc2 = acc.clone();
    let stage1 = block_reduce(defs, key, default, binop, move |x| {
        let mut out = acc2.writer();
        out.extend(&mut x.into_iter());
        out.finish()
    });

    // Split into chunks
    //let chunks = partition_by_key::<<Acc as Accumulator<(K, B)>>::VW as ValueWriter<(K, B)>>::Out>,_,_,_>(&stage1, partitions, |x| x.0.clone());
    let chunks = partition_by_key::<Acc,_,_,_>(&stage1, partitions, |x| x.0.clone());

    // partition reduce
    let concat: Vec<_> = chunks.into_iter().map(|chunk| {
        batch_apply(&chunk, |_idx, vs| {
            let mut hm = HashMap::new();
            for (k, v) in vs {
                hm.insert(k.clone(), v.clone());
            }
            hm
        })
    }).collect();

    let mut reduction = Vec::new();
    let nf = move |l: &HashMap<K,B>, r: &HashMap<K,B>| {
        merge_maps(l, r, reduce.clone())
    };
    for group in concat {
        let out = tree_reduce(&group, nf.clone());
        reduction.push(out.unwrap());
    }

    batch_apply(&reduction, move |_idx, vs| {
        let mut out = acc.writer();
        for (k, v) in vs {
            out.add((k.clone(), v.clone()));
        }
        out.finish()
    })
}

pub fn partition_by_key<
    C: Any + Sync + Send + Clone + Accumulator<A>,
    A: Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    F: 'static + Sync + Send + Clone + Fn(&A) -> K
>(
    defs: &[Deferred<C>], 
    n_chunks: usize, 
    key: F
) -> Vec<Vec<Deferred<C>>>
        where for<'a> &'a C: IntoIterator<Item=&'a A>, 
              C::VW: ValueWriter<A,Out=C> {
    split_by_key(defs, n_chunks, move |_idx, v| {
        let k = key(v);
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    })
}

pub fn concat<
    Col: Any + Sync + Send + Clone + Accumulator<A>,
    A: Clone,
>(
    defs: &[Deferred<Col>]
) -> Option<Deferred<Col>>
        where for<'a> &'a Col: IntoIterator<Item=&'a A>, 
              Col::VW: ValueWriter<A,Out=Col> {

    tree_reduce(&defs, |x, y| {
        let mut out = x.writer();
        for xi in x {
            out.add(xi.clone());
        }
        for yi in y {
            out.add(yi.clone());
        }
        out.finish()
    })
}



pub fn join_on_key<
    Col1: Any + Sync + Send + Clone,
    Col2: Any + Sync + Send + Clone,
    A, 
    B,
    K: Any + Send + Sync + Clone + Hash + Eq,
    C: Any + Sync + Send + Clone,
    J: 'static + Sync + Send + Clone + Fn(&A, &B) -> C,
    Acc: 'static + Accumulator<(K, C)>
>(
    d1: &Deferred<Col1>, 
    d2: &Deferred<Col2>, 
    acc: Acc,
    joiner: J
) -> Deferred<<<Acc as Accumulator<(K, C)>>::VW as ValueWriter<(K, C)>>::Out> 
        where for<'a> &'a Col1: IntoIterator<Item=&'a (K, A)>,
              for<'b> &'b Col2: IntoIterator<Item=&'b (K, B)> {

    d1.join(d2, move |left, right| {
        // Slurp up left into a hashmap
        let mut hm = HashMap::new();
        for (k, lv) in left {
            let e = hm.entry(k).or_insert_with(|| Vec::with_capacity(1)); 
            e.push(lv);
        }
        let mut ret = acc.writer();
        for (k, rv) in right {
            if let Some(lvs) = hm.get(k) {
                for lv in lvs.iter() {
                    ret.add((k.clone(), joiner(lv, rv)))
                }
            }
        }
        ret.finish()
    })
}

