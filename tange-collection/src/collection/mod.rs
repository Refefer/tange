pub mod memory;

use std::any::Any;

use tange::deferred::{Deferred, batch_apply};
use interfaces::{Accumulator,ValueWriter};

fn emit<
    A,
    Col: Any + Send + Sync + Clone,
    B: Any + Send + Sync + Clone,
    F: 'static + Sync + Send + Clone + Fn(&A, &mut FnMut(B) -> ()),
    Acc: 'static + Accumulator<B>
>(defs: &[Deferred<Col>], acc: Acc, f: F) -> Vec<Deferred<<<Acc as Accumulator<B>>::VW as ValueWriter<B>>::Out>>
        where for<'a> &'a Col: IntoIterator<Item=&'a A> {

    batch_apply(&defs, move |_idx, vs| {
        let mut out = acc.writer();
        for v in vs.into_iter() {
            f(v, &mut |r| out.add(r));
        }
        out.finish()
    })
}

