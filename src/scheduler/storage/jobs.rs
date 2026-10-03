//! Explicit return of exclusive job controls after every scheduler and backend
//! reference has gone away.

use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use super::super::{Envelope, OwnedScheduler, Record};
use crate::runtime::config::SWExecutionClass;

#[cfg(test)]
#[path = "../../../tests/unit/job_reuse.rs"]
mod tests;

pub(crate) struct Job {
    id: u64,
    scheduler: Weak<OwnedScheduler>,
    envelope: Mutex<Option<Envelope>>,
    class: SWExecutionClass,
    group_id: Option<u64>,
    record: Mutex<Option<Record>>,
}

impl Job {
    pub(in crate::scheduler) fn class(&self) -> SWExecutionClass {
        self.class
    }
    pub(in crate::scheduler) fn group_id(&self) -> Option<u64> {
        self.group_id
    }
    pub(in crate::scheduler) fn record_lock(&self) -> MutexGuard<'_, Option<Record>> {
        self.record
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
    pub(in crate::scheduler) fn id(&self) -> u64 {
        self.id
    }

    pub(in crate::scheduler) fn take_envelope(&self) -> Option<Envelope> {
        self.envelope
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }
}

struct Recycled {
    free: Vec<Arc<Job>>,
    limit: usize,
}

/// A job handle is the only way to retain a job control. All releases serialize
/// at the recycler so exactly one release observes the exclusive Arc. Checked
/// weak upgrades use that same exclusion. Normal settlement removes group and
/// demand associations and detaches prerequisite subscriptions before the final
/// strong release; retained public controls use class/ID and separate signals.
pub(crate) struct JobHandle {
    job: Option<Arc<Job>>,
    recycled: Arc<Mutex<Recycled>>,
}

/// A checked non-owning association. Public producer controls keep class/ID
/// instead, so retained observers cannot prevent exclusive storage reuse.
pub(crate) struct JobWeak {
    id: u64,
    job: Weak<Job>,
    recycled: Arc<Mutex<Recycled>>,
}

impl JobWeak {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    pub(crate) fn upgrade(&self) -> Option<JobHandle> {
        let _recycler = self
            .recycled
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let job = self.job.upgrade()?;
        if job.id != self.id {
            return None;
        }
        Some(JobHandle {
            job: Some(job),
            recycled: Arc::clone(&self.recycled),
        })
    }
}

impl JobHandle {
    pub(in crate::scheduler) fn downgrade(&self) -> JobWeak {
        JobWeak {
            id: self.id(),
            job: Arc::downgrade(self.job.as_ref().expect("live job handle")),
            recycled: Arc::clone(&self.recycled),
        }
    }
    pub(in crate::scheduler) fn initialize(
        &mut self,
        class: SWExecutionClass,
        group_id: Option<u64>,
        envelope: Envelope,
        record: Record,
    ) {
        let job = Arc::get_mut(self.job.as_mut().expect("live job handle"))
            .expect("prepared control is exclusive");
        job.class = class;
        job.group_id = group_id;
        *job.envelope
            .get_mut()
            .unwrap_or_else(|error| error.into_inner()) = Some(envelope);
        *job.record
            .get_mut()
            .unwrap_or_else(|error| error.into_inner()) = Some(record);
    }
    pub(in crate::scheduler) fn run(&self) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.run_job(self);
        }
    }
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
            let settled = control
                .record
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .is_none();
            if empty && settled && recycled.free.len() < recycled.limit {
                recycled.free.push(job);
                return;
            }
        } else {
            // Another handle remains. Releasing it under this mutex ensures
            // that one final drop will see an exclusive Arc.
            if Arc::strong_count(&job) == 1 {
                // Weak associations can outlive the last strong owner during
                // teardown. Its payload and accounting cleanup run unlocked.
                drop(recycled);
            }
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

    #[cfg(test)]
    pub(in crate::scheduler) fn acquire(
        &self,
        id: u64,
        scheduler: Weak<OwnedScheduler>,
        envelope: Envelope,
    ) -> JobHandle {
        self.acquire_slot(id, scheduler, Some(envelope))
    }

    pub(in crate::scheduler) fn prepare(
        &self,
        id: u64,
        scheduler: Weak<OwnedScheduler>,
    ) -> JobHandle {
        self.acquire_slot(id, scheduler, None)
    }

    pub(in crate::scheduler) fn prepare_many(
        &self,
        ids: &[u64],
        scheduler: Weak<OwnedScheduler>,
    ) -> Vec<JobHandle> {
        let mut returned = Vec::with_capacity(ids.len());
        {
            let mut recycled = self
                .recycled
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            for _ in ids {
                let Some(job) = recycled.free.pop() else {
                    break;
                };
                returned.push(job);
            }
        }
        let mut returned = returned.into_iter();
        ids.iter()
            .map(|&id| self.prepare_slot(returned.next(), id, scheduler.clone(), None))
            .collect()
    }

    fn acquire_slot(
        &self,
        id: u64,
        scheduler: Weak<OwnedScheduler>,
        envelope: Option<Envelope>,
    ) -> JobHandle {
        let recycled = {
            self.recycled
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .free
                .pop()
        };
        self.prepare_slot(recycled, id, scheduler, envelope)
    }

    fn prepare_slot(
        &self,
        recycled: Option<Arc<Job>>,
        id: u64,
        scheduler: Weak<OwnedScheduler>,
        envelope: Option<Envelope>,
    ) -> JobHandle {
        let mut job = recycled.unwrap_or_else(|| {
            Arc::new(Job {
                id: 0,
                scheduler: Weak::new(),
                envelope: Mutex::new(None),
                class: SWExecutionClass::Low,
                group_id: None,
                record: Mutex::new(None),
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
        *slot = envelope;
        JobHandle {
            job: Some(job),
            recycled: Arc::clone(&self.recycled),
        }
    }
}
