//! Controlled interleaving tests for the event's real notification and wait loops.
#![allow(clippy::missing_docs_in_private_items)]

use super::Event;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const DEADLINE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    AfterIncrement,
    BeforeWait(usize),
}

type Callback = dyn Fn(Point, u64) + Send + Sync;

pub(super) struct Hook {
    callback: Mutex<Option<Arc<Callback>>>,
}

impl fmt::Debug for Hook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hook").finish_non_exhaustive()
    }
}

impl Default for Hook {
    fn default() -> Self {
        Self::new()
    }
}

impl Hook {
    pub(super) const fn new() -> Self {
        Self {
            callback: Mutex::new(None),
        }
    }

    fn set(&self, callback: impl Fn(Point, u64) + Send + Sync + 'static) {
        *self.callback.lock().unwrap() = Some(Arc::new(callback));
    }

    fn call(&self, point: Point, version: u64) {
        let callback = self.callback.lock().unwrap().clone();
        if let Some(callback) = callback {
            callback(point, version);
        }
    }

    pub(super) fn after_increment(&self) {
        self.call(Point::AfterIncrement, 0);
    }

    pub(super) fn before_wait(&self, version: u64) {
        self.call(Point::BeforeWait(0), version);
    }
}

fn receive<T>(receiver: &mpsc::Receiver<T>, phase: &str) -> Result<T, String> {
    receiver
        .recv_timeout(DEADLINE)
        .map_err(|error| format!("{phase}: {error}"))
}

fn expect_point(receiver: &mpsc::Receiver<Point>, expected: Point) -> Result<(), String> {
    let actual = receive(receiver, "test hook")?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!("expected {expected:?}, got {actual:?}"))
    }
}

#[test]
fn listener_rearms_when_prior_notifier_clears_its_flag() {
    let event = Event::new();
    let mut old_listener = event.listen();
    let (point_send, point_receive) = mpsc::channel();
    let (notifier_release, notifier_resume) = mpsc::channel();
    let (wait_release, wait_resume) = mpsc::channel();
    let notifier_resume = Mutex::new(notifier_resume);
    let wait_resume = Mutex::new(wait_resume);
    let increment_count = AtomicUsize::new(0);
    let wait_count = AtomicUsize::new(0);
    event.test_hook.set(move |point, version| match point {
        Point::AfterIncrement if increment_count.fetch_add(1, Ordering::Relaxed) == 0 => {
            let _ = point_send.send(Point::AfterIncrement);
            let _ = notifier_resume.lock().unwrap().recv_timeout(DEADLINE);
        }
        Point::BeforeWait(_) if version == 1 => {
            let attempt = wait_count.fetch_add(1, Ordering::Relaxed) + 1;
            if attempt <= 2 {
                let _ = point_send.send(Point::BeforeWait(attempt));
                let _ = wait_resume.lock().unwrap().recv_timeout(DEADLINE);
            }
        }
        _ => {}
    });

    thread::scope(|scope| {
        let event_ref = &event;
        let old = scope.spawn(move || old_listener.spin_wait(0));
        let cancel = Arc::new(AtomicBool::new(false));
        let mut notifier = None;
        let mut fresh = None;
        let (notifier_done_send, notifier_done) = mpsc::channel();
        let (fresh_ready_send, fresh_ready) = mpsc::channel();
        let (fresh_done_send, fresh_done) = mpsc::channel();

        let result = (|| -> Result<(), String> {
            let deadline = Instant::now() + DEADLINE;
            while event.atomic.load(Ordering::Relaxed) & Event::WAITER_FLAG == 0 {
                if Instant::now() >= deadline {
                    return Err("old listener did not arm".into());
                }
                thread::yield_now();
            }

            let notifier_done_send = notifier_done_send.clone();
            let event_for_notifier = event_ref;
            notifier = Some(scope.spawn(move || {
                event_for_notifier.notify();
                let _ = notifier_done_send.send(());
            }));
            expect_point(&point_receive, Point::AfterIncrement)?;

            // The new listener saves version 1 while the old waiter flag is set.
            let mut listener = event_ref.listen();
            let fresh_ready_send = fresh_ready_send.clone();
            let fresh_done_send = fresh_done_send.clone();
            let fresh_cancel = Arc::clone(&cancel);
            fresh = Some(scope.spawn(move || {
                listener.wait();
                let _ = fresh_ready_send.send(());
                if !fresh_cancel.load(Ordering::Relaxed) {
                    listener.wait();
                }
                let _ = fresh_done_send.send(());
            }));
            expect_point(&point_receive, Point::BeforeWait(1))?;

            // Finish the old notification before the new listener attempts its wait.
            notifier_release
                .send(())
                .map_err(|error| error.to_string())?;
            receive(&notifier_done, "first notification")?;
            wait_release.send(()).map_err(|error| error.to_string())?;
            expect_point(&point_receive, Point::BeforeWait(2))?;
            if event.atomic.load(Ordering::Relaxed) & Event::WAITER_FLAG == 0 {
                return Err("listener retried without re-arming the waiter flag".into());
            }

            // A notification before the second OS wait must still release it.
            event.notify();
            wait_release.send(()).map_err(|error| error.to_string())?;
            receive(&fresh_ready, "first fresh wait")?;

            // Reuse the same listener. This notification can precede its next wait.
            event.notify();
            receive(&fresh_done, "reused listener")?;
            Ok(())
        })();

        if result.is_err() {
            cancel.store(true, Ordering::Relaxed);
        }
        let _ = notifier_release.send(());
        let _ = wait_release.send(());
        let _ = wait_release.send(());
        event.notify();
        event.notify();
        old.join().unwrap();
        if let Some(notifier) = notifier {
            notifier.join().unwrap();
        }
        if let Some(fresh) = fresh {
            fresh.join().unwrap();
        }
        result.unwrap();
    });
}
