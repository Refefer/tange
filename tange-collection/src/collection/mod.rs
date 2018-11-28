//! Contains the two main primitives: MemoryCollection and DiskCollection

/// Defines MemoryCollection and assorted functions
pub mod memory;

/// Defines DiskCollection and assorted functions
pub mod disk;

use std::any::Any;

use tange::deferred::{Deferred, batch_apply};
use interfaces::{Accumulator,ValueWriter,Stream};

fn emit<
    A,
    Col: Any + Send + Sync + Clone + Stream<A>,
    B: Any + Send + Sync + Clone,
    F: 'static + Sync + Send + Clone + Fn(&A, &mut FnMut(B) -> ()),
    Acc: 'static + Accumulator<B>
>(defs: &[Deferred<Col>], acc: Acc, f: F) -> Vec<Deferred<<<Acc as Accumulator<B>>::VW as ValueWriter<B>>::Out>> {

    batch_apply(&defs, move |_idx, vs| {
        let mut out = acc.writer();
        for v in vs.stream().into_iter() {
            f(&v, &mut |r| out.add(r));
        }
        out.finish()
    })
}

