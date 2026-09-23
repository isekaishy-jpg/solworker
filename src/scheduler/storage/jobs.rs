//! Explicit return of exclusive job controls after every scheduler and backend
//! reference has gone away.

use std::ops::Deref;
use std::sync::{Arc, Mutex, Weak};

use super::super::{Envelope, OwnedScheduler};

#[cfg(test)]
#[path = "../../../tests/unit/job_reuse.rs"]
mod tests;

pub(in crate::scheduler) struct Job {
    id: u64,
    scheduler: Weak<OwnedScheduler>,
    envelope: Mutex<Option<Envelope>>,
}

impl Job {
    pub(in crate::scheduler) fn id(&self) -> u64 {
        self.id
    }

    pub(in crate::scheduler) fn take_envelope(&self) -> Option<Envelope> {
        self.envelope
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    pub(in crate::scheduler) fn run(&self) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.run_job(self);
        }
    }
}

struct Recycled {
    free: Vec<Arc<Job>>,
    limit: usize,
}

/// A job handle is the only way to retain a job control. All releases serialize
/// at the recycler so exactly one release observes the exclusive Arc. No weak
/// job accessor exists; public task identities live in separate signals.
pub(in crate::scheduler) struct JobHandle {
    job: Option<Arc<Job>>,
    recycled: Arc<Mutex<Recycled>>,
}

impl Clone for JobHandle {
    fn clone(&self) -> Self {
        Self {
            job: Some(Arc::clone(self.job.as_ref().expect("live job handle"))),
            recycled: Arc::clone(&self.recycled),
        }
    }
}

impl Deref for JobHandle {
    type Target = Job;

    fn deref(&self) -> &Self::Target {
        self.job.as_deref().expect("live job handle")
    }
}

impl Drop for JobHandle {
    fn drop(&mut self) {
        let mut job = self.job.take().expect("live job handle");
        let mut recycled = self
            .recycled
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(control) = Arc::get_mut(&mut job) {
            // A settled record consumes its envelope before publishing the
            // completion and removing its last scheduler reference. A job
            // abandoned with an intact envelope must never enter the cache.
            let empty = control
                .envelope
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .is_none();
            if empty && recycled.free.len() < recycled.limit {
                recycled.free.push(job);
                return;
            }
        } else {
            // Another handle remains. Releasing it under this mutex ensures
            // that one final drop will see an exclusive Arc.
            drop(job);
            return;
        }
        drop(recycled);
        drop(job);
    }
}

/// Holds only returned, exclusive control allocations. Checkout never probes
/// live jobs or backend wrappers.
pub(in crate::scheduler) struct JobPool {
    recycled: Arc<Mutex<Recycled>>,
}

impl JobPool {
    pub(in crate::scheduler) fn new(limit: usize) -> Self {
        Self {
            recycled: Arc::new(Mutex::new(Recycled {
                free: Vec::new(),
                limit,
            })),
        }
    }

    pub(in crate::scheduler) fn acquire(
        &self,
        id: u64,
        scheduler: Weak<OwnedScheduler>,
        envelope: Envelope,
    ) -> JobHandle {
        let recycled = {
            self.recycled
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .free
                .pop()
        };
        let mut job = recycled.unwrap_or_else(|| {
            Arc::new(Job {
                id: 0,
                scheduler: Weak::new(),
                envelope: Mutex::new(None),
            })
        });
        let control = Arc::get_mut(&mut job).expect("returned job control is exclusive");
        control.id = id;
        control.scheduler = scheduler;
        let slot = control
            .envelope
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        debug_assert!(slot.is_none());
        *slot = Some(envelope);
        JobHandle {
            job: Some(job),
            recycled: Arc::clone(&self.recycled),
        }
    }
}
