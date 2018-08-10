use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

use task::{BASS,DynRun};

static GLOBAL_HANDLE_COUNT: AtomicUsize = ATOMIC_USIZE_INIT;

/// Interface for providing inputs into the graph, such as reading a file
pub trait Input: Send + Sync {
    fn read(&self) -> BASS;
}

/// Unique values representing a task in a Graph
#[derive(Debug,Clone,PartialEq,Eq,Hash)]
pub struct Handle(String, usize);

impl Handle {
    /// Creates a new handle.  
    fn new(name: String) -> Self {
        Handle(name, GLOBAL_HANDLE_COUNT.fetch_add(1, Ordering::SeqCst))
    }
}

/// ADT for handling either Tasks or reading data into the graph
pub enum Task {

    /// Node which consumes down stream data to produce new data
    Function(Box<DynRun>),
    
    /// Node which generates data
    Input(Box<Input>)
}

/// Holds references to the number of arguments to pass into a Task
#[derive(Debug,Clone)]
pub enum FnArgs {

    /// Single argument
    Single(Arc<Handle>),

    /// Used for joining two separate task outputs
    Join(Arc<Handle>, Arc<Handle>)
}

/// Graphs contain the computational pieces needed to represent the data flow
/// between multiple different tasks, their combination, and eventual output.
#[derive(Clone)]
pub struct Graph {

    /// Output handle to task
    pub tasks: HashMap<Arc<Handle>, Arc<Task>>,

    /// Dependencies between tasks
    pub dependencies: HashMap<Arc<Handle>, Option<Arc<FnArgs>>>
}

impl Graph {

    /// Creates a new Graph
    pub fn new() -> Self {
        Graph { 
            tasks: HashMap::new(), 
            dependencies: HashMap::new() 
        }
    }

    /// Adds a new input into the Graph
    pub fn add_input<I: Input + 'static>(&mut self, input: I, name: &str) -> Arc<Handle> {
        let i_name = format!("Input<id={},name={}>", self.tasks.len(), name);
        let handle = Arc::new(Handle::new(i_name));
        let inp = Arc::new(Task::Input(Box::new(input)));
        self.dependencies.insert(handle.clone(), None);
        self.tasks.insert(handle.clone(), inp);
        handle
    }

    /// Adds a task to the dataset with the given inputs.  No effort is made to ensure the
    /// handles exist within the graph.
    pub fn add_task<D: 'static + DynRun>(&mut self, inputs: FnArgs, t: D, name: &str) -> Arc<Handle> {
        // Get new handle
        let h_name = format!("Task<id={},name={}>", self.tasks.len(), name);
        let handle = Arc::new(Handle::new(h_name));
        let task = Task::Function(Box::new(t));
        self.dependencies.insert(handle.clone(), Some(Arc::new(inputs.clone())));
        self.tasks.insert(handle.clone(), Arc::new(task));
        handle
    }

    /// Given two graphs, merge all tasks and dependencies.
    pub fn merge(&self, other: &Graph) -> Graph {
        let mut nh = self.clone();

        // Add missing inputs
        for (handle, input) in other.dependencies.iter() {
            if !self.dependencies.contains_key(handle) {
                nh.dependencies.insert(handle.clone(), input.clone());
            }
        }

        // Add missing stages
        for (handle, task) in other.tasks.iter() {
            if !self.tasks.contains_key(handle) {
                nh.tasks.insert(handle.clone(), task.clone());
            }
        }
        nh
    }
}

