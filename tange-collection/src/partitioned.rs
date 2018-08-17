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
    Col: Any + Sync + Send + Clone,
    A: Any + Send + Sync + Clone,
    F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
>(
    defs: &[Deferred<Col>], 
    partitions: usize, 
    hash_function: F
) -> Vec<Vec<Deferred<Vec<A>>>> 
        where for<'a> &'a Col: IntoIterator<Item=&'a A> {

    // Group into buckets 
    let stage1 = batch_apply(&defs, move |_idx, vs| {
        let mut parts = vec![Memory.writer(); partitions];
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
    Col: Any + Sync + Send + Clone,
    A: Any + Send + Sync + Clone,
    F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
>(
    defs: &[Deferred<Col>], 
    partitions: usize, 
    key: F
) -> Vec<Deferred<Vec<A>>>
        where for<'a> &'a Col: IntoIterator<Item=&'a A> {
    
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

/*
pub fn fold_by<
    Col: Any + Sync + Send + Clone,
    A,
    B: Any + Sync + Send + Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    D: 'static + Sync + Send + Clone + Fn() -> B, 
    F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
    O: 'static + Sync + Send + Clone + Fn(&B, &A) -> B,
    R: 'static + Sync + Send + Clone + Fn(&B, &B) -> B
>(
    defs: &[Deferred<Col>],
    key: F, 
    default: D, 
    binop: O, 
    reduce: R, 
    partitions: usize
) -> Vec<Deferred<Vec<(K,B)>>> 
        where for<'a> &'a Col: IntoIterator<Item=&'a A> {

    let output = if partitions == 1 {
        // Cheaper to perform since we don't need to convert to and from HashMaps
        // and vecs, can avoid the split, etc.
        let stage1 = block_reduce(defs, key, default, binop, |x| x);
    
        let reduction = tree_reduce(&stage1, move |l, r| merge_maps(l, r, reduce.clone()));

        // Flatten
        batch_apply(&vec![reduction.unwrap()], |_idx, vs| {
            vs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        })

    } else {
        // Local reduce, split by key, partition reduce
        let stage1 = block_reduce(defs, key, default, binop, 
                                       |x| x.into_iter().collect::<Vec<(K,B)>>());

        // Split into chunks
        let chunks = partition_by_key(&stage1, partitions, |x| x.0.clone());

        // partition reduce
        let concat: Vec<_> = chunks.into_iter().map(|chunk| {
            batch_apply(&chunk, |_idx, vs| {
                let mut hm = HashMap::new();
                for (k, v) in vs.iter() {
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
            //let out = tree_reduce(&group, move |l, r| merge_maps(l, r, red));
            reduction.push(out.unwrap());
        }

        // Flatten
        batch_apply(&reduction, |_idx, vs| {
            vs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        })
    };

    output
}
*/

pub fn fold_by<
    Col: Any + Sync + Send + Clone,
    A,
    B: Any + Sync + Send + Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    D: 'static + Sync + Send + Clone + Fn() -> B, 
    F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
    O: 'static + Sync + Send + Clone + Fn(&B, &A) -> B,
    R: 'static + Sync + Send + Clone + Fn(&B, &B) -> B,
    Acc: 'static + Accumulator<(K, B)>
>(
    defs: &[Deferred<Col>],
    key: F, 
    default: D, 
    binop: O, 
    reduce: R, 
    acc: Acc,
    partitions: usize
) -> Vec<Deferred<Vec<(K,B)>>> 
        where for<'a> &'a Col: IntoIterator<Item=&'a A> {

    let output = if partitions == 1 {
        // Cheaper to perform since we don't need to convert to and from HashMaps
        // and vecs, can avoid the split, etc.
        let stage1 = block_reduce(defs, key, default, binop, |x| x);
    
        let reduction = tree_reduce(&stage1, move |l, r| merge_maps(l, r, reduce.clone()));

        // Flatten
        batch_apply(&vec![reduction.unwrap()], |_idx, vs| {
            vs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        })

    } else {
        // Local reduce, split by key, partition reduce
        let stage1 = block_reduce(defs, key, default, binop, 
                                       |x| x.into_iter().collect::<Vec<(K,B)>>());

        // Split into chunks
        let chunks = partition_by_key(&stage1, partitions, |x| x.0.clone());

        // partition reduce
        let concat: Vec<_> = chunks.into_iter().map(|chunk| {
            batch_apply(&chunk, |_idx, vs| {
                let mut hm = HashMap::new();
                for (k, v) in vs.iter() {
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
            //let out = tree_reduce(&group, move |l, r| merge_maps(l, r, red));
            reduction.push(out.unwrap());
        }

        // Flatten
        batch_apply(&reduction, |_idx, vs| {
            vs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        })
    };

    output
}

pub fn partition_by_key<
    A: Any + Sync + Send + Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    F: 'static + Sync + Send + Clone + Fn(&A) -> K
>(
    defs: &[Deferred<Vec<A>>], 
    n_chunks: usize, 
    key: F
) -> Vec<Vec<Deferred<Vec<A>>>> {
    split_by_key(defs, n_chunks, move |_idx, v| {
        let k = key(v);
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    })
}

pub fn concat<
    A: Any + Sync + Send + Clone,
    >(defs: &[Deferred<Vec<A>>]) -> Option<Deferred<Vec<A>>> {
    tree_reduce(&defs, |x, y| {
        let mut v1: Vec<_> = (*x).clone();
        for yi in y {
            v1.push(yi.clone());
        }
        v1
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
