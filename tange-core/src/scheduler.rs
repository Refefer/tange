extern crate num_cpus;
extern crate rayon;
extern crate log;
extern crate priority_queue;
extern crate jobpool;

use std::sync::{Mutex,Arc,mpsc};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use log::Level::{Trace,Debug as LDebug};
use self::rayon::prelude::*;
use self::priority_queue::PriorityQueue;
use self::jobpool::JobPool;

use task::{BASS,DynArgs};
use graph::{Graph,Task,Handle,FnArgs};

type DepGraph = HashMap<Arc<Handle>, HashSet<Arc<Handle>>>; 
type ChainGraph = HashMap<Vec<Arc<Handle>>, HashSet<Arc<Handle>>>; 

#[derive(Debug)]
struct DataStore<K: PartialEq + Hash + Eq, V> {
    data: HashMap<K, V>,
    counts: HashMap<K, usize>
}

impl <K: PartialEq + Hash + Eq, V: Clone> DataStore<K,V> {
    fn new(
        data: HashMap<K, V>, 
        counts: HashMap<K, usize>
    ) -> Self {
        DataStore {data: data, counts: counts}
    }

    fn get(&mut self, handle: &K) -> Option<V> {
        let count = self.counts.get_mut(handle).map(|c| {
            *c -= 1;
            *c
        }).unwrap_or(0);

        if count == 0 {
            self.data.remove(handle)
        } else {
            self.data.get(handle).map(|x| x.clone())
        }
    }

    fn insert(&mut self, handle: K, data: V) {
        self.data.insert(handle, data);
    }
}


pub trait Scheduler {
    fn compute(&mut self, graph: Arc<Graph>) -> Option<Arc<BASS>>; 
}

enum Limbo {
    One(Arc<BASS>),
    Two(Arc<BASS>, Arc<BASS>)
}

struct DAG {
   
    /// Output handle to task
    pub tasks: HashMap<Arc<Handle>, Arc<Task>>,

    /// Dependencies between tasks
    pub dependencies: HashMap<Arc<Handle>, Option<FnArgs>>
 
}

impl DAG {
    fn new(g: Arc<Graph>) -> Self {
        let mut tasks = HashMap::new();
        let mut dependencies = HashMap::new();

        let mut stack = vec![g];

        let mut hs = HashSet::new();

        while !stack.is_empty() {
            trace!("Stack size: {}", stack.len());
            let ag = stack.pop().unwrap();
            if !hs.contains(&ag.handle) {
                hs.insert(ag.handle.clone());
                tasks.insert(ag.handle.clone(), ag.task.clone());
                dependencies.insert(ag.handle.clone(), ag.args.clone());
                if let Some(ref fns) = ag.args {
                    match fns {
                        FnArgs::Single(g) => stack.push(g.clone()),
                        FnArgs::Join(g1, g2) => {
                            stack.push(g1.clone());
                            stack.push(g2.clone());
                        }
                    };
                }
            }
        }
        DAG {
            tasks: tasks,
            dependencies: dependencies
        }
    }
}

fn get_fnargs(ds: &mut DataStore<Arc<Handle>,Arc<BASS>>, fa: &FnArgs) -> Option<Limbo> {
    match fa {
        &FnArgs::Single(ref g) => {
            ds.get(&g.handle).map(|args| {
                Limbo::One(args)
            })
        },
        &FnArgs::Join(ref lg, ref rg) => {
            ds.get(&lg.handle).and_then(|left| {
                ds.get(&rg.handle).map(|right| {
                    Limbo::Two(left, right)
                })
            })
        }
    }
}

// Converts a flattened graph into a dependency list
fn build_dep_graph(graph: &DAG) -> (DepGraph, DepGraph) {
    // Build out dependencies
    let mut inbound: DepGraph = HashMap::new();
    let mut outbound: DepGraph = HashMap::new();
    for (output, ref inputs) in graph.dependencies.iter() {
        let mut hs = HashSet::new();
        if let Some(inp) = inputs {
            let fna: &FnArgs = &inp;
            match fna {
                &FnArgs::Single(ref h) => hs.insert(h.handle.clone()),
                &FnArgs::Join(ref h1, ref h2) => {
                    hs.insert(h1.handle.clone());
                    hs.insert(h2.handle.clone())
                },
            };
        }
        // Add outbound
        for h in hs.iter() {
            let e = outbound.entry(h.clone()).or_insert_with(|| HashSet::with_capacity(1));
            e.insert(output.clone());
        }
        inbound.insert(output.clone(), hs);
    }
    inbound.shrink_to_fit();
    outbound.shrink_to_fit();
    (inbound, outbound)
}

// Constructs a set of nodes that have no dependencies between them
fn generate_levels(collapsed: ChainGraph) -> Vec<Vec<Vec<Arc<Handle>>>> {
    // Create outbound
    let mut outbound = HashMap::new();
    for (nodes, deps) in collapsed.iter() {
        for d in deps.iter() {
            let e = outbound.entry(d).or_insert_with(|| HashSet::with_capacity(1));
            e.insert(nodes);
        }
    }
    let mut inbound = collapsed.clone();
    // Compute task levels
    let mut levels = Vec::new();
    let mut cur_level: Vec<Vec<Arc<Handle>>> = inbound.iter()
            .filter(|(_, v)| v.is_empty())
            .map(|(k, _)| k.clone())
            .collect();

    loop {
        
        if cur_level.is_empty() {
            break;
        }

        // Remove nodes from graph
        for handles in cur_level.iter() {
            inbound.remove(handles);
        }

        // Update dependencies
        let mut next_level = Vec::new();
        for hs in cur_level.iter() {
            // Get outbound nodes
            let last = &hs[hs.len() - 1];
            if let Some(node_set) = outbound.get(last) {
                for node in node_set.iter() {
                    if let Some(set) = inbound.get_mut(*node) {
                        set.remove(last);
                        if set.is_empty() {
                            next_level.push((*node).clone());
                        }
                    }
                }
            }
        }

        levels.push(cur_level);
        cur_level = next_level;
    }
    if log_enabled!(LDebug) {
        let mut max_con = 0usize;
        for (i, l) in levels.iter().enumerate() {
            max_con = max_con.max(l.len());
            debug!("Level: {}, Tasks: {}", i, l.len());
        }
        debug!("Max Concurrency: {}", max_con);
    }
    levels
}

fn run_task(
    graph: &DAG, 
    chain: &[Arc<Handle>], 
    dsam: Arc<Mutex<DataStore<Arc<Handle>, Arc<BASS>>>> 
) {
    // Pull out arguments from the datasource
    trace!("Reading dependencies for chain {:?}", chain[0]);
    let ot = graph.dependencies.get(&chain[0]);
    let mut largs = {
        let ds: &mut DataStore<_,_> = &mut *dsam.lock().unwrap();
        // Get inputs
        match ot {
            Some(Some(ar)) => get_fnargs(ds, &ar),
            _              => None
        }
    };

    for handle in chain {
        trace!("Processing handle: {:?}", handle);
        let out = match graph.tasks.get(handle) {
            Some(ref task) => {
                let task_ref: &Task = &task;
                match task_ref {
                    Task::Input(ref input) => Some(input.read()),
                    Task::Function(ref t) => {
                        match largs {
                            Some(Limbo::One(ref a)) => {
                                t.eval(DynArgs::One(a))
                            },
                            Some(Limbo::Two(ref a, ref b)) => {
                                t.eval(DynArgs::Two(a, b))
                            },
                            None => None
                        }
                    }
                }
            },
            None => None
        };
        if let Some(bass) = out {
            largs = Some(Limbo::One(Arc::new(bass)));
        }
    }

    if let Some(Limbo::One(d)) = largs {
        let mut ds = dsam.lock().unwrap();
        ds.insert(chain[chain.len() - 1].clone(), d);
    } 
}

// Finds chains of tasks that can be collapsed into a single task
use std::fmt::Debug;
fn collapse_graph<K: Hash + Eq + Debug + Clone>(
    mut nodes: HashMap<K, HashSet<K>>
) -> HashMap<Vec<K>, HashSet<K>> {

    // Generate outbound edges
    let mut outbound = HashMap::new();
    let mut roots = Vec::new();
    let mut inbound: HashMap<K, Vec<K>> = HashMap::new();
    for (node, deps) in nodes.iter() {
        if !outbound.contains_key(node) {
            outbound.insert(node.clone(), Vec::new());
        }

        for d in deps.iter() {
            let e = outbound.entry(d.clone()).or_insert(Vec::new());
            e.push(node.clone());
        }

        if deps.is_empty() {
            roots.push(vec![node.clone()]);
        }

        inbound.insert(node.clone(), deps.iter().cloned().collect());
    }

    let mut new_nodes = HashMap::new();
    let mut seen = HashSet::new();
    while !roots.is_empty() {
        if let Some(mut chain) = roots.pop() {
            let link = {
                let tail = &chain[chain.len() - 1];

                // If outbound == 1 and that refernce only has one inbound
                if outbound[tail].len() == 1 && inbound[&outbound[tail][0]].len() == 1 {
                    // We found a link in a chain
                    // Add the node to the current list
                    Some(outbound[tail][0].clone())
                } else {
                    None
                    // Our chain is finished, emit it
                }
            };

            if let Some(node) = link {
                chain.push(node);
                roots.push(chain);
            } else {
                // If current chain is ended, add the outbound nodes
                {
                    let tail = &chain[chain.len() - 1];
                    for node in outbound[tail].iter() {
                        if !seen.contains(node) {
                            roots.push(vec![node.clone()]);
                            seen.insert(node.clone());
                        }
                    }
                }
                // Emit current chain
                let deps = nodes.remove(&chain[0]).unwrap();
                new_nodes.insert(chain, deps);
            }
        }
    }

    new_nodes
}

pub struct LeveledScheduler;

impl Scheduler for LeveledScheduler{

    fn compute(
        &mut self, 
        graph: Arc<Graph>
    ) -> Option<Arc<BASS>> {
        
        let out_handle = graph.handle.clone();
        let dag = DAG::new(graph);
        debug!("Number of Tasks Specified: {}", dag.tasks.len());

        let (inbound, _outbound) = build_dep_graph(&dag);

        let collapsed = collapse_graph(inbound);

        debug!("Number of Tasks to Run: {}", collapsed.len());
        
        // Build the counts
        let mut counts: HashMap<Arc<Handle>,_> = HashMap::new();
        for (_k, vs) in collapsed.iter() {
            for v in vs.iter() {
                let e = counts.entry(v.clone()).or_insert(0usize);
                *e += 1;
            }
        }

        // Build out the levels
        let levels = generate_levels(collapsed);
        
        // Load up the inputs
        let data: HashMap<Arc<Handle>,Arc<BASS>> = HashMap::new();

        // Add all handles
        let raw_ds: DataStore<Arc<Handle>, Arc<BASS>> = DataStore::new(data, counts);
        let dsam = Arc::new(Mutex::new(raw_ds));

        for (i, level) in levels.into_iter().enumerate() {
            debug!("Running level: {}", i);
            // Run graph
            level.par_iter().for_each(|chain| { run_task(&dag, chain, dsam.clone())})
                
        }

        debug!("Finished");
        let ret = {
            dsam.lock().unwrap().get(&out_handle)
        };
        ret
    }
}

pub struct GreedyScheduler(usize);

impl GreedyScheduler {
    pub fn new() -> Self {
        GreedyScheduler(num_cpus::get())
    }
    pub fn set_threads(&mut self, n_threads: usize) -> () {
         self.0 = n_threads;
    }
}

impl Scheduler for GreedyScheduler {

    fn compute(
        &mut self, 
        graph: Arc<Graph>
    ) -> Option<Arc<BASS>> {
        
        let out_handle = graph.handle.clone();

        trace!("Building Dag...");
        let dag = Arc::new(DAG::new(graph));
        
        debug!("Number of Tasks Specified: {}", dag.tasks.len());

        let (inbound, mut outbound) = build_dep_graph(&dag);

        let collapsed = collapse_graph(inbound);

        let total_jobs = collapsed.len();
        debug!("Number of Tasks to Run: {}", total_jobs);
        
        // Build the counts
        let mut counts: HashMap<Arc<Handle>,_> = HashMap::new();
        let mut queue = PriorityQueue::new();
        for (chain, deps) in collapsed.iter() {
            // Add the inputs
            if deps.len() == 0 {
                trace!("Adding intial chain: {:?}, Priority: {}", chain, 0usize);
                queue.push(chain.clone(), 0usize);
            }

            for d in deps.iter() {
                let e = counts.entry(d.clone()).or_insert(0usize);
                *e += 1;
            }
        }

        // Make the graph a bit easier to work with
        let mut head_map: HashMap<_,_> = collapsed.into_iter().map(|(chain, deps)| {
            (chain[0].clone(), (chain, deps.len(), deps))
        }).collect();

        // Load up the inputs
        let data: HashMap<Arc<Handle>,Arc<BASS>> = HashMap::new();

        // Initialize an empty data store
        let raw_ds: DataStore<Arc<Handle>, Arc<BASS>> = DataStore::new(data, counts);
        let dsam = Arc::new(Mutex::new(raw_ds));

        // Start the loop!

        if log_enabled!(Trace) {
            for (ref index, &(ref chain, ref _priority, ref deps)) in head_map.iter() {
                trace!("Index: {:?}, Chain: {:?}, Deps: {:?}", index, chain, deps);
            }
        }
        debug!("Starting tasks...");
        let mut jobs_done = 0usize;
        {
            let mut pool = JobPool::new(self.0);
            let mut free_threads = self.0;
            let (tx, rx) = mpsc::channel();
            loop {
                // Queue up all free items
                while free_threads > 0 && !queue.is_empty(){
                    if let Some((chain, priority)) = queue.pop() {
                        trace!("Training chain: {:?}, Priority: {}", chain, priority);
                        let g = dag.clone();
                        let c = chain.clone();
                        let d = dsam.clone();
                        let thread_tx = tx.clone();
                        pool.queue(move || {
                            run_task(&g, &c, d);
                            thread_tx.send(c[c.len() - 1].clone())
                                .expect("Error sending thread!");
                        });
                        free_threads -= 1;
                    } 
                }

                // Eat!
                let handle = rx.recv().unwrap(); 
                // Remove it as deps from remaining tasks
                trace!("{:?} finished", handle);
                free_threads += 1;
                if let Some(out) = outbound.remove(&handle) {
                    for out_handle in out {
                        trace!("Updating {:?}", out_handle);
                        if let Some((chain, p, deps)) = head_map.get_mut(&out_handle) {
                            trace!("Updating {:?}", out_handle);
                            deps.remove(&handle);
                            if deps.is_empty() {
                                trace!("Adding new chain: {:?}, Priority: {}", chain, p);
                                queue.push(chain.clone(), *p);
                            } else {
                                trace!("Remaining Deps: {:?}", deps);
                            }
                        }
                    }
                }

                jobs_done += 1;
                if total_jobs > 10 && jobs_done % (total_jobs as f64 / 10.) as usize == 0 {
                    debug!("Finished {}/{} of jobs", jobs_done, total_jobs);
                    if log_enabled!(Trace) {
                        let ds = dsam.lock().unwrap();
                        trace!("Data Chunks in memory: {}", ds.data.len());
                    }

                }
                // Are we done yet?
                if free_threads == self.0 && queue.is_empty() {
                    break
                }
            }
            pool.shutdown();
        }

        if log_enabled!(Trace) {
            let ds = dsam.lock().unwrap();
            trace!("Still Holding data for:");
            for (k, _v) in ds.data.iter() {
                trace!("- {:?}", k);
            }
        }

        debug!("Finished");
        let ret = {
            dsam.lock().unwrap().get(&out_handle)
        };
        ret
    }
}

#[cfg(test)]
mod size_test {
    use super::*;

    #[test]
    fn test_graph_collapse() {
        /*
        1 -> 2 -> 3
              \
               4 -> 5

        We should collapse 1 -> 2 and 4 -> 5
        */
        let one_deps = HashSet::new();
        let mut two_deps = HashSet::new();
        two_deps.insert(1usize);

        let mut three_deps = HashSet::new();
        three_deps.insert(2usize);

        let mut four_deps = HashSet::new();
        four_deps.insert(2usize);

        let mut five_deps = HashSet::new();
        five_deps.insert(4usize);

        let mut deps = HashMap::new();
        deps.insert(1usize, one_deps);
        deps.insert(2usize, two_deps);
        deps.insert(3usize, three_deps);
        deps.insert(4usize, four_deps);
        deps.insert(5usize, five_deps);

        let out = collapse_graph(deps);
        let mut res = HashMap::new();
        res.insert(vec![1, 2], vec![].iter().cloned().collect());
        res.insert(vec![3], vec![2].iter().cloned().collect());
        res.insert(vec![4, 5], vec![2].iter().cloned().collect());

        assert_eq!(out, res);
    }

    #[test]
    fn test_graph_collapse_2() {
        /*
             2 -> 4
            /     |
           1 ---> 3

        */
        let one_deps = HashSet::new();
        let mut two_deps = HashSet::new();
        two_deps.insert(1usize);

        let mut three_deps = HashSet::new();
        three_deps.insert(1usize);

        let mut four_deps = HashSet::new();
        four_deps.insert(2usize);
        four_deps.insert(3usize);

        let mut deps = HashMap::new();
        deps.insert(1usize, one_deps);
        deps.insert(2usize, two_deps);
        deps.insert(3usize, three_deps);
        deps.insert(4usize, four_deps);

        let res = deps.clone().into_iter().map(|(k, v)| (vec![k], v)).collect();
        let out = collapse_graph(deps);

        assert_eq!(out, res);
    }

}
