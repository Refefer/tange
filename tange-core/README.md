Tange
===

A Task-based parallelization framework.

What is it?
---

`Tange` is a framework that makes it easy to write defered, data parallel computations that are executed concurrently across a local machine.  It can scale up to millions of tasks per Graph and can be useful for a number of different applications:

* Data processing.
* All-Reduce operations.
* Distributed machine learning algorithms.
* General parallel computing.

How to Use It?
---

Tange defines a `Deferred` struct which represents a computation.  `Deferred` objects are accessed with three simple functions:

1. `lift` - Lift takes a concrete value and lifts it into a Deferred object
2. `apply` - Apply applies a function to a Deferred, producing a new Deferred object.
3. `join` -  Join combines two Deferred objects with a joiner function, producing a new Deferred.

Example - Hello World
---

```rust
use tange::deferred::Deferred;
use tange::scheduler::GreedyScheduler;

// Create two Deferred object
let hello = Deferred::lift("Hello".to_owned(), None);
let world = Deferred::lift("World".to_owned(), None);

// Add an exclamation mark to "World"
let world_exclaim = world.apply(|w| format!("{}!", w));

// Join the words!
let hello_world = hello.join(&world_exclaim, |h, w| format!("{} {}", h, w));

assert_eq!(hello_world.run(&GreedyScheduler::new()), Some("Hello World!".into()));
```

Example
---

Let's count all the words across a directory.

```rust
extern crate tange;

use tange::scheduler::GreedyScheduler;
use tange::deferred::{Deferred,batch_apply,tree_reduce};

use std::io::{BufReader,BufRead};
use std::env::args;

use std::io;
use std::fs::{File, read_dir};
use std::path::Path;

fn read_files(dir: &Path, buffer: &mut Vec<Deferred<String>>) -> io::Result<()> {
    if dir.is_dir() {
        for entry in read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                read_files(&path, buffer)?;
            } else {
                let p = path.to_string_lossy().into_owned();
                buffer.push(Deferred::lift(p, None));
            }
        }
    }
    Ok(())
}

fn main() {
    let mut defs = Vec::new();
    for path in args().skip(1) {
        read_files(&Path::new(&path), &mut defs).expect("Error reading directory!");
    }

    if defs.len() == 0 {
        panic!("No files to count!");
    }

    // Read a file and count the number of words, split by white space
    let counts = batch_apply(&defs, |_idx, fname| {
        let mut count = 0usize;
        if let Ok(f) = File::open(&fname) {
            let mut br = BufReader::new(f);
            for maybe_line in br.lines() {
                if let Ok(line) = maybe_line {
                    for p in line.split_whitespace() {
                        if p.len() > 0 {
                            count += 1;
                        }
                    }
                } else {
                    eprintln!("Error reading {}, skipping rest of file...", fname);
                    break
                }
            }
        };
        count
    });

    // Sum the counts
    let total = tree_reduce(&counts, |left, right| left + right)
        .expect("Can't reduce if there are no files in the directory!");

    let count = total.run(&GreedyScheduler::new()).unwrap();
    println!("Found {} words", count);
}
```
