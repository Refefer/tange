extern crate tange;

use std::any::Any;
use std::hash::{Hasher,Hash};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;

use tange::deferred::{Deferred, batch_apply, tree_reduce};

pub fn block_reduce<
    A: Any + Sync + Send + Clone,
    K: Any + Sync + Send + Clone + Hash + Eq,
    B,
    C: Any + Sync + Send + Clone,
    D: 'static + Sync + Send + Clone + Fn() -> B, 
    F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
    O: 'static + Sync + Send + Clone + Fn(&B, &A) -> B,
    M: 'static + Sync + Send + Clone + Fn(HashMap<K,B>) -> C,
>(
    defs: &[Deferred<Vec<A>>], 
    key: F, 
    default: D, 
    binop: O,
    map: M
) -> Vec<Deferred<C>> {
    batch_apply(defs, move |_idx, vs| {
        let mut reducer = HashMap::new();
        for v in vs {
            let k = key(v);
            let e = reducer.entry(k).or_insert_with(&default);
            *e = binop(e, v);
        }
        map(reducer)
    })
}

pub fn split_by_key<
    A: Any + Send + Sync + Clone,
    B: Any + Send + Sync + Clone,
    I: 'static + Sync + Send + Clone + Fn(&A) -> Box<Iterator<Item=B>>,
    F: 'static + Sync + Send + Clone + Fn(usize, &B) -> usize
>(
    defs: &[Deferred<A>], 
    partitions: usize, 
    make_iter: I,
    hash_function: F
) -> Vec<Vec<Deferred<Vec<B>>>> {

    // Group into buckets 
    let stage1 = batch_apply(&defs, move |_idx, vs| {
        let mut parts = vec![Vec::new(); partitions];
        for (idx, x) in make_iter(vs).enumerate() {
            let p = hash_function(idx, &x) % partitions;
            parts[p].push(x);
        }
        parts
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
    A: Any + Send + Sync + Clone,
    B: Any + Send + Sync + Clone,
    I: 'static + Sync + Send + Clone + Fn(&A) -> Box<Iterator<Item=B>>,
    F: 'static + Sync + Send + Clone + Fn(usize, &B) -> usize
>(
    defs: &[Deferred<A>], 
    partitions: usize, 
    iter: I,
    key: F
) -> Vec<Deferred<Vec<B>>> {
    
    let groups = split_by_key(defs, partitions, iter, key);
    
    let mut new_chunks = Vec::with_capacity(groups.len());
    for group in groups {
        let output = tree_reduce(&group, |x, y| {
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
   A: Any + Send + Sync + Clone,
   B: Any + Sync + Send + Clone,
   K: Any + Sync + Send + Clone + Hash + Eq,
   D: 'static + Sync + Send + Clone + Fn() -> B, 
   F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
   O: 'static + Sync + Send + Clone + Fn(&B, &A) -> B,
   R: 'static + Sync + Send + Clone + Fn(&B, &B) -> B
>(
    defs: &[Deferred<Vec<A>>],
    key: F, 
    default: D, 
    binop: O, 
    reduce: R, 
    partitions: usize
) -> Vec<Deferred<Vec<(K,B)>>> {

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
    split_by_key(defs, n_chunks, |v| Box::new(v.clone().into_iter()), move |_idx, v| {
        let k = key(v);
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    })
}

pub fn concat<A: Any + Sync + Send + Clone >(defs: &[Deferred<Vec<A>>]) -> Deferred<Vec<A>> {
    tree_reduce(&defs, |x, y| {
        let mut v1: Vec<_> = (*x).clone();
        for yi in y {
            v1.push(yi.clone());
        }
        v1
    }).unwrap()
}
