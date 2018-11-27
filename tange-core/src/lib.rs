//! tange-core
//!
//! `tange-core` provides primitives for building and running task-based computations.
//!  
//! What is it?
//! ---
//! 
//! `Tange` is a framework that makes it easy to write defered, data parallel computations that are executed concurrently across a local machine.  It can scale up to millions of tasks per Graph and can be useful for a number of different applications:
//! 
//! * Data processing.
//! * All-Reduce operations.
//! * Distributed machine learning algorithms.
//! * General parallel computing.
//! 
//! How to Use It?
//! ---
//! 
//! Tange defines a `Deferred` struct which represents a computation.  `Deferred` objects are accessed with three simple functions:
//! 
//! 1. `lift` - Lift takes a concrete value and lifts it into a Deferred object
//! 2. `apply` - Apply applies a function to a Deferred, producing a new Deferred object.
//! 3. `join` -  Join combines two Deferred objects with a joiner function, producing a new Deferred.
//! 
//! Example - Hello World!
//! ---
//! ```rust
//! use tange::deferred::Deferred;
//! use tange::scheduler::GreedyScheduler;
//! 
//! let hello = Deferred::lift("Hello".to_owned(), None);
//! let world = Deferred::lift("World".to_owned(), None);
//! let world_exclaim = world.apply(|w| format!("{}!", w));
//! let hello_world = hello.join(&world_exclaim, |h, w| format!("{} {}", h, w));
//! assert_eq!(hello_world.run(&GreedyScheduler::new()), Some("Hello World!".into()));
//! ```
//! 
//! 
//! 

#![warn(missing_docs)]

#[macro_use]
extern crate log;

/// Contains Deferred primitive and function definitions
pub mod deferred;

/// Contains Scheduler trait definition and implementations
pub mod scheduler;

/// Internal Graph implementation
mod graph;

/// Internal task definitions
mod task;

