extern crate serde;
use std::fs;
use std::any::Any;
use std::io::prelude::*;
use std::io::BufWriter;
use std::hash::Hash;
use std::sync::Arc;

use self::serde::Deserialize;
use self::serde::Serialize;

use tange::deferred::{Deferred, batch_apply, tree_reduce};
use tange::scheduler::Scheduler;

use collection::memory::MemoryCollection;
use partitioned::{join_on_key as jok, partition, partition_by_key, fold_by, concat};
use interfaces::*;
use super::emit;


#[derive(Clone)]
pub struct DiskCollection<A: Clone + Send + Sync>  {
    path: Arc<String>,
    partitions: Vec<Deferred<FileStore<A>>>
}

impl <A: Any + Send + Sync + Clone + Serialize + for<'de>Deserialize<'de>> DiskCollection<A> {

    pub fn from_vec(path: String, vec: Vec<A>) -> DiskCollection<A> {
        MemoryCollection::from_vec(vec).to_disk(path)
    }

    pub fn from_memory(path: String, mc: &Vec<Deferred<Vec<A>>>) -> DiskCollection<A> {
        ::std::fs::create_dir_all(&path).expect("Unable to create directory!");
        let shared = Arc::new(path);
        let acc = FileStore::empty(shared.clone());
        let defs = batch_apply(&mc, move |_idx, vs| {
            acc.write_vec(vs.clone())
        });
        DiskCollection { path: shared, partitions: defs }
    }

    pub fn from_stores(path: String, fs: Vec<Deferred<FileStore<A>>>) -> DiskCollection<A> {
        DiskCollection { path: Arc::new(path), partitions: fs }
    }

    pub fn to_defs(&self) -> &Vec<Deferred<FileStore<A>>> {
        &self.partitions
    }

    pub fn to_memory(&self) -> MemoryCollection<A> {
        let defs = batch_apply(&self.partitions, |_idx, vs| {
            vs.stream().into_iter().collect()
        });
        MemoryCollection::from_defs(defs)
    }

    pub fn n_partitions(&self) -> usize {
        self.partitions.len()
    }

    fn from_defs<B: Clone + Send + Sync>(&self, defs: Vec<Deferred<FileStore<B>>>) -> DiskCollection<B> {
        DiskCollection { path: self.path.clone(), partitions: defs }
    }

    pub fn concat(&self, other: &DiskCollection<A>) -> DiskCollection<A> {
        let mut nps: Vec<_> = self.partitions.iter()
            .map(|p| (*p).clone()).collect();

        for p in other.partitions.iter() {
            nps.push(p.clone());
        }

        self.from_defs(nps)
    }
    
    pub fn map<
        B: Any + Send + Sync + Clone + Serialize, 
        F: 'static + Sync + Send + Clone + Fn(&A) -> B
    >(&self, f: F) -> DiskCollection<B> {
        self.emit(move |x, emitter| {
            emitter(f(x))
        })
    }

    pub fn filter<
        F: 'static + Sync + Send + Clone + Fn(&A) -> bool
    >(&self, f: F) -> DiskCollection<A> {
        self.emit(move |x, emitter| {
            if f(x) { 
                emitter(x.clone())
            }
        })
    }
    
    pub fn split(&self, n_chunks: usize) -> DiskCollection<A> {
        self.partition(n_chunks, |idx, _k| idx)
    }

    pub fn emit<
        B: Any + Send + Sync + Clone + Serialize,
        F: 'static + Sync + Send + Clone + Fn(&A, &mut FnMut(B) -> ())
    >(&self, f: F) -> DiskCollection<B> {

        let parts = emit(&self.partitions, Disk(self.path.clone()), f);

        self.from_defs(parts)
    }

    pub fn partition<
        F: 'static + Sync + Send + Clone + Fn(usize, &A) -> usize
    >(&self, partitions: usize, f: F) -> DiskCollection<A> {
        let new_chunks = partition(&self.partitions, 
                                   partitions, 
                                   f);
        // Loop over each bucket
        self.from_defs(new_chunks)
    }

    pub fn fold_by<K: Any + Sync + Send + Clone + Hash + Eq + Serialize + for<'de> Deserialize<'de>,
                   B: Any + Sync + Send + Clone + Serialize + for<'de> Deserialize<'de>,
                   D: 'static + Sync + Send + Clone + Fn() -> B,
                   F: 'static + Sync + Send + Clone + Fn(&A) -> K, 
                   O: 'static + Sync + Send + Clone + Fn(&mut B, &A) -> (),
                   R: 'static + Sync + Send + Clone + Fn(&mut B, &B) -> ()>(
        &self, key: F, default: D, binop: O, reduce: R, partitions: usize
    ) -> DiskCollection<(K,B)> {
        let results = fold_by(&self.partitions, key, default, binop, 
                              reduce, FileStore::empty(self.path.clone()), partitions);
        self.from_defs(results)
    }

    pub fn partition_by_key<
        K: Any + Sync + Send + Clone + Hash + Eq,
        F: 'static + Sync + Send + Clone + Fn(&A) -> K
    >(&self, n_chunks: usize, key: F) -> DiskCollection<A> {
        let results = partition_by_key(&self.partitions, n_chunks, key);
        let groups = results.into_iter().map(|part| concat(&part).unwrap()).collect();
        self.from_defs(groups)
    }

    pub fn sort_by<
        K: Ord,
        F: 'static + Sync + Send + Clone + Fn(&A) -> K
    >(&self, key: F) -> DiskCollection<A> {
        let acc = FileStore::empty(self.path.clone());
        let nps = batch_apply(&self.partitions, move |_idx, vs| {
            let mut out = acc.writer();
            let mut v2: Vec<_> = vs.stream().into_iter().collect();
            v2.sort_by_key(|v| key(v));
            for vi in v2 {
                out.add(vi);
            }
            out.finish()
        });
        self.from_defs(nps)
    }

    pub fn join_on<
        K: Any + Sync + Send + Clone + Hash + Eq + Serialize + for<'de> Deserialize<'de>,
        B: Any + Sync + Send + Clone + Serialize + for<'de> Deserialize<'de>,
        C: Any + Sync + Send + Clone + Serialize,
        KF1: 'static + Sync + Send + Clone + Fn(&A) -> K,
        KF2: 'static + Sync + Send + Clone + Fn(&B) -> K,
        J:   'static + Sync + Send + Clone + Fn(&A, &B) -> C,
    >(
        &self, 
        other: &DiskCollection<B>, 
        key1: KF1, 
        key2: KF2,
        joiner: J,
        partitions: usize, 
    ) -> DiskCollection<(K,C)> {
        // Group each by a common key
        let p1 = self.map(move |x| (key1(x), x.clone()))
            .partition_by_key(partitions, |x| x.0.clone());
        let p2 = other.map(move |x| (key2(x), x.clone()))
            .partition_by_key(partitions, |x| x.0.clone());

        let mut new_parts = Vec::with_capacity(p1.partitions.len());
        for (l, r) in p1.partitions.iter().zip(p2.partitions.iter()) {
            let acc = FileStore::empty(self.path.clone());
            new_parts.push(jok(l, r, acc, joiner.clone()));
        }

        self.from_defs(new_parts)
    }

    pub fn run<S: Scheduler>(&self, s: &mut S) -> Option<Vec<A>> {
        let defs = batch_apply(&self.partitions, |_idx, vs| {
            vs.stream().into_iter().collect::<Vec<_>>()
        });
        let cat = tree_reduce(&defs, |x, y| {
            let mut v1: Vec<_> = (*x).clone();
            for yi in y {
                v1.push(yi.clone());
            }
            v1
        });
        cat.and_then(|x| x.run(s))
    }
}

impl <A: Any + Send + Sync + Clone + Serialize + for<'de>Deserialize<'de>> DiskCollection<Vec<A>> {
    pub fn flatten(&self) -> DiskCollection<A> {
        self.emit(move |x, emitter| {
            for xi in x {
                emitter(xi.clone());
            }
        })
    }
}

impl <A: Any + Send + Sync + Clone + Serialize + for<'de>Deserialize<'de>> DiskCollection<A> {
    pub fn count(&self) -> DiskCollection<usize> {
        let nps = batch_apply(&self.partitions, |_idx, vs| {
            vs.stream().into_iter().map(|_| 1usize).sum::<usize>()
        });
        let count = tree_reduce(&nps, |x, y| x + y).unwrap();
        let acc = FileStore::empty(self.path.clone());
        let out = count.apply(move |x| {
            acc.write_vec(vec![*x])
        });
        self.from_defs(vec![out])
    }
}

impl <A: Any + Send + Sync + Clone + PartialEq + Hash + Eq + Serialize + for<'de>Deserialize<'de>> DiskCollection<A> {
    pub fn frequencies(&self, partitions: usize) -> DiskCollection<(A, usize)> {
        //self.partition(chunks, |x| x);
        self.fold_by(|s| s.clone(), 
                     || 0usize, 
                     |acc, _l| *acc += 1, 
                     |x, y| *x += *y, 
                     partitions)
    }
}

// Writes out data
impl DiskCollection<String> {
    pub fn sink(&self, path: &'static str) -> DiskCollection<usize> {
        let acc = FileStore::empty(self.path.clone());
        let pats = batch_apply(&self.partitions, move |idx, vs| {
            fs::create_dir_all(path)
                .expect("Welp, something went terribly wrong when creating directory");

            let file = fs::File::create(&format!("{}/{}", path, idx))
                .expect("Issues opening file!");
            let mut bw = BufWriter::new(file);

            let mut size = 0usize;
            for line in vs.stream() {
                bw.write(line.as_bytes()).expect("Error writing out line");
                bw.write(b"\n").expect("Error writing out line");
                size += 1;
            }

            acc.write_vec(vec![size])
        });
        
        self.from_defs(pats)
    }
}

#[cfg(test)]
mod test_lib {
    use super::*;
    use tange::scheduler::{GreedyScheduler,LeveledScheduler};

    fn make_col() -> DiskCollection<usize> {
        DiskCollection::from_vec("/tmp".into(), vec![1,2,3,1,2usize])
    }

    #[test]
    fn test_fold_by() {
        let col = make_col();
        let out = col.fold_by(|x| *x, || 0, |x, _y| *x += 1, |x, y| *x += y, 1);
        let mut results = out.run(&mut LeveledScheduler).unwrap();
        results.sort();
        assert_eq!(results, vec![(1, 2), (2, 2), (3, 1)]);
    }

    #[test]
    fn test_fold_by_parts() {
        let col = make_col();
        let out = col.fold_by(|x| *x, || 0, |x, _y| *x += 1, |x, y| *x += y, 2);
        assert_eq!(out.partitions.len(), 2);
        let mut results = out.run(&mut LeveledScheduler).unwrap();
        results.sort();
        assert_eq!(results, vec![(1, 2), (2, 2), (3, 1)]);
    }

    #[test]
    fn test_partition_by_key() {
        let col = make_col();
        let computed = col.partition_by_key(2, |x| *x)
            .sort_by(|x| *x);
        assert_eq!(computed.partitions.len(), 2);
        let results = computed.run(&mut LeveledScheduler).unwrap();
        assert_eq!(results, vec![2, 2, 3, 1, 1]);
    }

    #[test]
    fn test_partition() {
        let col = make_col();
        let computed = col.partition(2, |_idx, x| x % 2)
            .sort_by(|x| *x);
        assert_eq!(computed.partitions.len(), 2);
        let results = computed.run(&mut GreedyScheduler::new()).unwrap();
        assert_eq!(results, vec![2, 2, 1, 1, 3]);
    }

    #[test]
    fn test_count() {
        let col = make_col();
        let results = col.split(3).count().run(&mut LeveledScheduler).unwrap();
        assert_eq!(results, vec![5]);
    }

    #[test]
    fn test_join() {
        let col1 = make_col();
        let col2 = DiskCollection::from_vec("/tmp".into(),
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
        let results = DiskCollection::from_vec("/tmp".into(), vec![1,2,3usize])
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

    /*
    #[test]
    fn test_sort() {
        let results = DiskCollection::from_vec(vec![1, 3, 2usize])
            .sort_by(|x| *x)
            .run(&mut LeveledScheduler).unwrap();
        let expected = vec![1, 2, 3];
        assert_eq!(results, expected);
    }

    */
}
