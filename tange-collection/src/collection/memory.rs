//! MemoryCollection
//! ---
//! MemoryCollection provides a variety of dataflow operators for consuming and mutating
//! data.  Unlike its Disk-based counterpart, DiskCollection, MemoryCollection keeps all
//! data in memory, maximizing speed.
//!

extern crate serde;
use std::fs;
use std::any::Any;
use std::io::prelude::*;
use std::io::BufWriter;
use std::hash::Hash;
use std::sync::Arc;

use self::serde::{Deserialize,Serialize};

use collection::disk::DiskCollection;
use tange::deferred::{Deferred, batch_apply, tree_reduce};
use tange::scheduler::{Scheduler,GreedyScheduler};
use partitioned::{join_on_key as jok, partition, partition_by_key, fold_by, concat};
use interfaces::{Memory,Disk};
use super::emit;


/// MemoryCollection struct
#[derive(Clone)]
pub struct MemoryCollection<A>  {
    partitions: Vec<Deferred<Vec<A>>>
}

impl <A: Any + Send + Sync + Clone> MemoryCollection<A> {

    /// Creates a MemoryCollection from a set of Deferred objects.
    pub fn from_defs(vs: Vec<Deferred<Vec<A>>>) -> MemoryCollection<A> {
        MemoryCollection {
            partitions: vs
        }
    }

    /// Provides raw access to the underlying Deferred objects
    pub fn to_defs(&self) -> &Vec<Deferred<Vec<A>>> {
        &self.partitions
    }

    /// Creates a new MemoryCollection from a Vec of items
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3usize]);
    ///   assert_eq!(col.run(&GreedyScheduler::new()), Some(vec![1,2,3usize]));
    /// ```
    pub fn from_vec(vs: Vec<A>) -> MemoryCollection<A> {
        MemoryCollection {
            partitions: vec![Deferred::lift(vs, None)],
        }
    }

    /// Returns the current number of data partitions 
    pub fn n_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Concatentates two collections into a single Collection
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let one = MemoryCollection::from_vec(vec![1,2,3usize]);
    ///   let two = MemoryCollection::from_vec(vec![4usize, 5, 6]);
    ///   let cat = one.concat(&two);
    ///   assert_eq!(cat.run(&GreedyScheduler::new()), Some(vec![1,2,3,4,5,6]));
    /// ```
    pub fn concat(&self, other: &MemoryCollection<A>) -> MemoryCollection<A> {
        let mut nps: Vec<_> = self.partitions.iter()
            .map(|p| (*p).clone()).collect();

        for p in other.partitions.iter() {
            nps.push(p.clone());
        }

        MemoryCollection { partitions: nps }
    }
    
    /// Maps a function over the values in the DiskCollection, returning a new DiskCollection
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let one = MemoryCollection::from_vec(vec![1,2,3usize]);
    ///   let strings = one.map(|i| format!("{}", i));
    ///   assert_eq!(strings.run(&GreedyScheduler::new()), 
    ///     Some(vec!["1".into(),"2".into(),"3".into()]));
    /// ```
    pub fn map<
        B: Any + Send + Sync + Clone, 
        F: 'static + Sync + Send + Clone + Fn(&A) -> B
    >(&self, f: F) -> MemoryCollection<B> {
        self.emit(move |x, emitter| {
            emitter(f(x))
        })
    }

    /// Filters out items in the collection that fail the predicate.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3usize]);
    ///   let odds = col.filter(|x| x % 2 == 1);
    ///   assert_eq!(odds.run(&GreedyScheduler::new()), 
    ///     Some(vec![1, 3usize]));
    /// ```

    pub fn filter<
        F: 'static + Sync + Send + Clone + Fn(&A) -> bool
    >(&self, f: F) -> MemoryCollection<A> {
        self.emit(move |x, emitter| {
            if f(x) { 
                emitter(x.clone())
            }
        })
    }
    
    /// Re-partitions a collection by the number of provided chunks.  It uniformly distributes data from each old partition into each new partition.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3usize]);
    ///   assert_eq!(col.n_partitions(), 1);
    ///   let two = col.split(2);
    ///   assert_eq!(two.n_partitions(), 2);
    /// ```
    pub fn split(&self, n_chunks: usize) -> MemoryCollection<A> {
        self.partition(n_chunks, |idx, _k| idx)
    }

    /// Maps over all items in a collection, optionally emitting new values.  It can be used
    /// to efficiently fuse a number of map/filter/flat_map functions into a single method.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3usize]);
    ///   let new = col.emit(|item, emitter| {
    ///     if item % 2 == 0 {
    ///         emitter(format!("{}!", item));
    ///     }
    ///   });
    ///   assert_eq!(new.run(&GreedyScheduler::new()), Some(vec!["2!".into()]));
    /// ```

    pub fn emit<
        B: Any + Send + Sync + Clone,
        F: 'static + Sync + Send + Clone + Fn(&A, &mut FnMut(B) -> ())
    >(&self, f: F) -> MemoryCollection<B> {
        let parts = emit(&self.partitions, Memory, f);

        MemoryCollection { partitions: parts }
    }

    /// Maps over all items in a collection, emitting new values.  It can be used
    /// to efficiently fuse a number of map/filter/flat_map functions into a single method.
    /// `emit_to_disk` differs from the original `emit` by writing the emitted values directly
    /// to disk, returning a DiskCollection instead of MemoryCollection.  This makes it convenient to switch to out-of-core when needed.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3usize]);
    ///   let new = col.emit_to_disk("/tmp".into(), |item, emitter| {
    ///     if item % 2 == 0 {
    ///         emitter(format!("{}!", item));
    ///     }
    ///   });
    ///   assert_eq!(new.run(&GreedyScheduler::new()), Some(vec!["2!".into()]));
    /// ```

    pub fn emit_to_disk<
        B: Any + Send + Sync + Clone + Serialize + for<'de>Deserialize<'de>,
        F: 'static + Sync + Send + Clone + Fn(&A, &mut FnMut(B) -> ())
    >(&self, path: String, f: F) -> DiskCollection<B> {
        let parts = emit(&self.partitions, Disk::from_str(&path), f);

        DiskCollection::from_stores(path, parts)
    }

    /// Re-partitions data into N new partitions by the given function.  The user provided
    /// function is used as a hash function, mapping the returned value to a partition index.
    /// This makes it useful for managing which partition data ends up!
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3,4usize]);
    ///   let new_col = col.partition(2, |idx, x| if *x < 3 { 1 } else { 2 });
    ///   
    ///   assert_eq!(new_col.n_partitions(), 2);
    ///   assert_eq!(new_col.run(&GreedyScheduler::new()), Some(vec![3, 4, 1, 2]));
    /// ```
    pub fn partition<
        F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
    >(&self, partitions: usize, f: F) -> MemoryCollection<A> {
        let new_chunks = partition(&self.partitions, 
                                   partitions, 
                                   f);
        // Loop over each bucket
        MemoryCollection { partitions: new_chunks }
    }

    /// Folds and accumulates values across multiple partitions into K new partitions.
    /// This is also known as a "group by" with a following reducer.
    ///
    /// MemoryCollection first performs a block aggregation: that is, it combines values
    /// within each partition first using the `binop` function.  It then hashes
    /// each key to a new partition index, where it will then aggregate all keys using the
    /// `reduce` function.
    ///
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3,4,5usize]);
    ///   // Sum all odds and evens together
    ///   let group_sum = col.fold_by(|x| x % 2,
    ///                               || 0usize,
    ///                               |block_acc, item| {*block_acc += *item},
    ///                               |part_acc1, part_acc2| {*part_acc1 += *part_acc2},
    ///                               1)
    ///                   .sort_by(|x| x.0);
    ///   
    ///   assert_eq!(group_sum.n_partitions(), 1);
    ///   assert_eq!(group_sum.run(&GreedyScheduler::new()), Some(vec![(0, 6), (1, 9)]));
    /// ```

    pub fn fold_by<K: Any + Sync + Send + Clone + Hash + Eq,
                   B: Any + Sync + Send + Clone,
                   D: 'static + Sync + Send + Clone + Fn() -> B, 
                   F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
                   O: 'static + Sync + Send + Clone + Fn(&mut B, &A) -> (),
                   R: 'static + Sync + Send + Clone + Fn(&mut B, &B) -> ()>(
        &self, key: F, default: D, binop: O, reduce: R, partitions: usize
    ) -> MemoryCollection<(K,B)> {
        let results = fold_by(&self.partitions, key, default, binop, 
                              reduce, Vec::with_capacity(0), partitions);
        MemoryCollection { partitions: results }
    }

    /// Simple function to re-partition values by a given key.  The return key is hashed
    /// and moduloed by the new partition count to determine where it will end up.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3,4usize]);
    ///   let new_col = col.partition_by_key(2, |x| format!("{}", x));
    ///   
    ///   assert_eq!(new_col.n_partitions(), 2);
    ///   assert_eq!(new_col.run(&GreedyScheduler::new()), Some(vec![4, 1, 2, 3]));
    /// ```
    pub fn partition_by_key<
        K: Any + Sync + Send + Clone + Hash + Eq,
        F: 'static + Sync + Send + Clone + Fn(&A) -> K
    >(&self, n_chunks: usize, key: F) -> MemoryCollection<A> {
        let results = partition_by_key(&self.partitions, n_chunks, key);
        let groups = results.into_iter().map(|part| concat(&part).unwrap()).collect();
        MemoryCollection {partitions: groups}
    }

    /// Sorts values within each partition by a key function.  If a global sort is desired,
    /// the collection needs to be re-partitioned into a single partition
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1,2,3,4i32]);
    ///   let new_col = col.sort_by(|x| -*x);
    ///   
    ///   assert_eq!(new_col.run(&GreedyScheduler::new()), Some(vec![4, 3, 2, 1]));
    /// ```
    pub fn sort_by<
        K: Ord,
        F: 'static + Sync + Send + Clone + Fn(&A) -> K
    >(&self, key: F) -> MemoryCollection<A> {
        let nps = batch_apply(&self.partitions, move |_idx, vs| {
            let mut v2: Vec<_> = vs.clone();
            v2.sort_by_key(|v| key(v));
            v2
        });
        MemoryCollection { partitions: nps }
    }

    /// Inner Joins two collections by the provided key function.
    /// If multiple values of the same key are found, they will be cross product for each
    /// pair found.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///
    ///   let name_age: Vec<(String,u32)> = vec![("Andrew".into(), 33), ("Leah".into(), 12)];
    ///   let name_money: Vec<(String,f32)> = vec![("Leah".into(), 20.50)];
    ///   
    ///   let na = MemoryCollection::from_vec(name_age);
    ///   let nm = MemoryCollection::from_vec(name_money);
    ///   let joined = na.join_on(&nm,
    ///                           |nax| nax.0.clone(),
    ///                           |nmx| nmx.0.clone(),
    ///                           |nax, nmx| (nax.0.clone(), nax.1, nmx.1),
    ///                           1);
    ///   assert_eq!(joined.run(&GreedyScheduler::new()), 
    ///           Some(vec![("Leah".into(), ("Leah".into(), 12, 20.50))]));
    /// ```

    pub fn join_on<
        K: Any + Sync + Send + Clone + Hash + Eq,
        B: Any + Sync + Send + Clone,
        C: Any + Sync + Send + Clone,
        KF1: 'static + Sync + Send + Clone + Fn(&A) -> K,
        KF2: 'static + Sync + Send + Clone + Fn(&B) -> K,
        J:   'static + Sync + Send + Clone + Fn(&A, &B) -> C,
    >(
        &self, 
        other: &MemoryCollection<B>, 
        key1: KF1, 
        key2: KF2,
        joiner: J,
        partitions: usize, 
    ) -> MemoryCollection<(K,C)> {
        // Group each by a common key
        let p1 = self.map(move |x| (key1(x), x.clone()))
            .partition_by_key(partitions, |x| x.0.clone());
        let p2 = other.map(move |x| (key2(x), x.clone()))
           .partition_by_key(partitions, |x| x.0.clone());

        let mut new_parts = Vec::with_capacity(p1.partitions.len());
        for (l, r) in p1.partitions.iter().zip(p2.partitions.iter()) {
            new_parts.push(jok(l, r, Memory, joiner.clone()));
        }

        MemoryCollection { partitions: new_parts }
    }

    /// Executes the Collection, returning the result of the computation
    pub fn run<S: Scheduler>(&self, s: &S) -> Option<Vec<A>> {
        let cat = tree_reduce(&self.partitions, |x, y| {
            let mut v1: Vec<_> = (*x).clone();
            for yi in y {
                v1.push(yi.clone());
            }
            v1
        });
        cat.and_then(|x| x.run(s))
    }
    
    /// Executes the Collection, returning the result of the computation
    pub fn eval(&self) -> Option<Vec<A>> {
        self.run(&GreedyScheduler::new())
    }

}

impl <A: Any + Send + Sync + Clone> MemoryCollection<Vec<A>> {

    /// Flattens a vector of values
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![vec![1usize,2],vec![3,4]]);
    ///   let flattened = col.flatten();
    ///   assert_eq!(flattened.run(&GreedyScheduler::new()), Some(vec![1, 2, 3, 4]));
    /// ```

    pub fn flatten(&self) -> MemoryCollection<A> {
        self.emit(move |x, emitter| {
            for xi in x {
                emitter(xi.clone());
            }
        })
    }
}

impl <A: Any + Send + Sync + Clone> MemoryCollection<A> {

    /// Returns the number of items in the collection.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![vec![1usize,2],vec![3,4]]);
    ///   assert_eq!(col.count().run(&GreedyScheduler::new()), Some(vec![2]));
    ///   let flattened = col.flatten();
    ///   assert_eq!(flattened.count().run(&GreedyScheduler::new()), Some(vec![4]));
    /// ```
    pub fn count(&self) -> MemoryCollection<usize> {
        let nps = batch_apply(&self.partitions, |_idx, vs| vs.len());
        let count = tree_reduce(&nps, |x, y| x + y).unwrap();
        let out = count.apply(|x| vec![*x]);
        MemoryCollection { partitions: vec![out] }
    }
}

impl <A: Any + Send + Sync + Clone + PartialEq + Hash + Eq> MemoryCollection<A> {

    /// Computes the frequencies of the items in collection.
    /// ```rust
    ///   extern crate tange;
    ///   extern crate tange_collection;
    ///   use tange::scheduler::GreedyScheduler;
    ///   use tange_collection::collection::memory::MemoryCollection;
    ///   
    ///   let col = MemoryCollection::from_vec(vec![1, 2, 1, 5, 1, 2]);
    ///   let freqs = col.frequencies(1).sort_by(|x| x.0);
    ///   assert_eq!(freqs.run(&GreedyScheduler::new()), Some(vec![(1, 3), (2, 2), (5, 1)]));
    /// ```
pub fn frequencies(&self, partitions: usize) -> MemoryCollection<(A, usize)> {
        //self.partition(chunks, |x| x);
        self.fold_by(|s| s.clone(), 
                     || 0usize, 
                     |acc, _l| *acc += 1, 
                     |x, y| *x += *y, 
                     partitions)
    }
}

// Writes out data
impl MemoryCollection<String> {

    /// Writes each record in a collection to disk, newline delimited.
    /// MemoryCollection will create a new file within the path for each partition.
    pub fn sink(&self, path: &str) -> MemoryCollection<usize> {
        let p: Arc<String> = Arc::new(path.to_owned());
        let pats = batch_apply(&self.partitions, move |idx, vs| {
            let p2: Arc<String> = p.clone();
            let local: &str = &p2;
            fs::create_dir_all(local)
                .expect("Welp, something went terribly wrong when creating directory");

            let file = fs::File::create(&format!("{}/{}", local, idx))
                .expect("Issues opening file!");
            let mut bw = BufWriter::new(file);

            let size = vs.len();
            for line in vs {
                bw.write(line.as_bytes()).expect("Error writing out line");
                bw.write(b"\n").expect("Error writing out line");
            }

            vec![size]
        });
        
        MemoryCollection { partitions: pats }
    }
}

impl <A: Any + Send + Sync + Clone + Serialize + for<'de>Deserialize<'de>> MemoryCollection<A> {

    /// Copies the MemoryCollection to disk, returning a DiskCollection
    pub fn to_disk(&self, path: String) -> DiskCollection<A> {
        DiskCollection::from_memory(path, &self.partitions)
    }
}

#[cfg(test)]
mod test_lib {
    use super::*;
    use tange::scheduler::LeveledScheduler;

    #[test]
    fn test_fold_by() {
        let col = MemoryCollection::from_vec(vec![1,2,3,1,2usize]);
        let out = col.fold_by(|x| *x, || 0, |x, _y| *x += 1, |x, y| *x += y, 1);
        let mut results = out.run(&mut LeveledScheduler).unwrap();
        results.sort();
        assert_eq!(results, vec![(1, 2), (2, 2), (3, 1)]);
    }

    #[test]
    fn test_fold_by_parts() {
        let col = MemoryCollection::from_vec(vec![1,2,3,1,2usize]);
        let out = col.fold_by(|x| *x, || 0, |x, _y| *x += 1, |x, y| *x += y, 2);
        assert_eq!(out.partitions.len(), 2);
        let mut results = out.run(&mut LeveledScheduler).unwrap();
        results.sort();
        assert_eq!(results, vec![(1, 2), (2, 2), (3, 1)]);
    }

    #[test]
    fn test_partition_by_key() {
        let col = MemoryCollection::from_vec(vec![1,2,3,1,2usize]);
        let computed = col.partition_by_key(2, |x| *x)
            .sort_by(|x| *x);
        assert_eq!(computed.partitions.len(), 2);
        let results = computed.run(&mut LeveledScheduler).unwrap();
        assert_eq!(results, vec![2, 2, 3, 1, 1]);
    }

    #[test]
    fn test_partition() {
        let col = MemoryCollection::from_vec(vec![1,2,3,1,2usize]);
        let computed = col.partition(2, |_idx, x| x % 2)
            .sort_by(|x| *x);
        assert_eq!(computed.partitions.len(), 2);
        let results = computed.run(&mut LeveledScheduler).unwrap();
        assert_eq!(results, vec![2, 2, 1, 1, 3]);
    }

    #[test]
    fn test_count() {
        let col = MemoryCollection::from_vec(vec![1,2,3,1,2usize]);
        let results = col.split(3).count().run(&mut LeveledScheduler).unwrap();
        assert_eq!(results, vec![5]);
    }

    #[test]
    fn test_join() {
        let col1 = MemoryCollection::from_vec(vec![1,2,3,1,2usize]);
        let col2 = MemoryCollection::from_vec(
            vec![(2, 1.23f64), (3usize, 2.34)]);
        let out = col1.join_on(&col2, |x| *x, |y| y.0, |x, y| {
            (*x, y.1)
        }, 5).split(1).sort_by(|x| x.0);
        let results = out.run(&mut LeveledScheduler).unwrap();
        let expected = vec![(2, (2, 1.23)), (2, (2, 1.23)), (3, (3, 2.34))];
        assert_eq!(results, expected);
    }

    #[test]
    fn test_emit() {
        let results = MemoryCollection::from_vec(vec![1,2,3usize])
            .emit(|num, emitter| {
                for i in 0..*num {
                    emitter(i);
                }
            })
            .sort_by(|x| *x)
            .run(&mut LeveledScheduler).unwrap();
        let expected = vec![0, 0, 0, 1, 1, 2];
        assert_eq!(results, expected);
    }

    #[test]
    fn test_sort() {
        let results = MemoryCollection::from_vec(vec![1, 3, 2usize])
            .sort_by(|x| *x)
            .run(&mut LeveledScheduler).unwrap();
        let expected = vec![1, 2, 3];
        assert_eq!(results, expected);
    }

}
