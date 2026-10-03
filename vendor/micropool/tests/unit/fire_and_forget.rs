//! Once-only execution/disposal of the actual inline callable representation.
#![allow(clippy::missing_docs_in_private_items)]

use super::{FireAndForgetTask, TaskInner};
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;

struct CountDrop(Arc<AtomicUsize>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn executors_and_disposal_compete_for_one_non_sync_callable() {
    for _ in 0..32 {
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = CountDrop(drops.clone());
        let value = Cell::new(37usize);
        let (sent, received) = mpsc::channel();
        let task = FireAndForgetTask {
            func: takecell::TakeOwnCell::new(move || {
                let _capture = capture;
                value.set(value.get() + 4);
                sent.send((value.get(), thread::current().id())).unwrap();
            }),
        };
        let start = Barrier::new(4);
        let executed = thread::scope(|scope| {
            let first = scope.spawn(|| {
                start.wait();
                task.run()
            });
            let second = scope.spawn(|| {
                start.wait();
                task.run()
            });
            let discard = scope.spawn(|| {
                start.wait();
                task.discard();
            });
            start.wait();
            let executed = usize::from(first.join().unwrap()) + usize::from(second.join().unwrap());
            discard.join().unwrap();
            executed
        });
        let messages = received.try_iter().collect::<Vec<_>>();
        assert!(executed <= 1);
        assert_eq!(messages.len(), executed);
        for (value, executor) in messages {
            assert_eq!(value, 41);
            assert_ne!(executor, thread::current().id());
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!task.run());
        task.discard();
        drop(task);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn panicking_callable_is_consumed_before_unwind_and_never_retried() {
    let drops = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = CountDrop(drops.clone());
    let invoked = calls.clone();
    let task = FireAndForgetTask {
        func: takecell::TakeOwnCell::new(move || {
            let _capture = capture;
            invoked.fetch_add(1, Ordering::SeqCst);
            panic!("callable failed");
        }),
    };
    assert!(catch_unwind(AssertUnwindSafe(|| task.run())).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(!task.run());
    task.discard();
    drop(task);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
