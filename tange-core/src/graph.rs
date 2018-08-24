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
#[derive(Clone)]
pub enum FnArgs {

    /// Single argument
    Single(Arc<Graph>),

    /// Used for joining two separate task outputs
    Join(Arc<Graph>, Arc<Graph>)
}

/// Graphs contain the computational pieces needed to represent the data flow
/// between multiple different tasks, their combination, and eventual output.
#[derive(Clone)]
pub struct Graph {

    pub handle: Arc<Handle>,

    pub task: Arc<Task>,

    pub args: Option<FnArgs>

}

impl Graph {

    /// Adds a new input into the Graph
    pub fn create_input<I: Input + 'static>(input: I, name: &str) -> Arc<Graph> {
        let i_name = format!("Input<name={}>", name);
        let handle = Arc::new(Handle::new(i_name));
        let inp = Arc::new(Task::Input(Box::new(input)));
        Arc::new(Graph {
            handle: handle,
            task: inp,
            args: None
        })
    }

    /// Adds a task to the dataset with the given inputs.  No effort is made to ensure the
    /// handles exist within the graph.
    pub fn create_task<D: 'static + DynRun>(inputs: FnArgs, t: D, name: &str) -> Arc<Graph> {
        // Get new handle
        let h_name = format!("Task<name={}>", name);
        let handle = Arc::new(Handle::new(h_name));
        let task = Arc::new(Task::Function(Box::new(t)));
        Arc::new(Graph {
            handle: handle,
            task: task,
            args: Some(inputs)
        })
    }

}

