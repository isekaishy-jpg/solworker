//! Invocation-owned, fixed-type preparation storage. Cache gates are isolated:
//! checkout/return never overlap lifecycle, ledger, class, record or recycler
//! guards. Active leases remain private even during reentrant callbacks.

use super::reservation::OwnedCapacity;
use super::storage::{JobCheckouts, SignalCheckouts};
use super::{AccountingLease, Finish, JobHandle, SWTaskStatus};
use crate::runtime::OwnedAdmission;
use crate::task::{SignalLease, Subscription};
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, TryLockError};

const IDLE_COUNT: usize = 2;
const IDLE_BYTES: usize = 256 * 1024;

#[cfg(test)]
#[path = "../../tests/unit/batch_scratch.rs"]
mod tests;

#[derive(Default)]
pub(super) struct Buffers {
    pub capacities: Vec<OwnedCapacity>,
    pub charges: Vec<AccountingLease>,
    pub jobs: Vec<JobHandle>,
    pub job_checkouts: JobCheckouts,
    pub signals: Vec<SignalLease>,
    pub signal_checkouts: SignalCheckouts,
    pub subscriptions: Vec<Vec<Subscription>>,
    pub portion: Vec<JobHandle>,
    pub parents: Vec<u64>,
    pub finishes: Vec<(usize, Finish)>,
    pub suppressed: Vec<(usize, SWTaskStatus)>,
    // Keep the exceptional auto-drop path runtime-last as well as clear().
    pub admissions: Vec<OwnedAdmission>,
}

impl Buffers {
    fn clear(&mut self) {
        // Runtime admissions are the final lifetime marker: refund all other
        // provisional credits and release application-owning controls first.
        for value in self.capacities.drain(..) {
            crate::cleanup::discard_value(value);
        }
        for value in self.charges.drain(..) {
            crate::cleanup::discard_value(value);
        }
        for value in self.jobs.drain(..) {
            crate::cleanup::discard_value(value);
        }
        self.job_checkouts.clear();
        for value in self.signals.drain(..) {
            crate::cleanup::discard_value(value);
        }
        self.signal_checkouts.clear();
        for value in self.subscriptions.drain(..) {
            crate::cleanup::discard_value(value);
        }
        for value in self.portion.drain(..) {
            crate::cleanup::discard_value(value);
        }
        self.parents.clear();
        for value in self.finishes.drain(..) {
            crate::cleanup::discard_value(value);
        }
        self.suppressed.clear();
        for value in self.admissions.drain(..) {
            crate::cleanup::discard_value(value);
        }
    }

    fn bytes(&self) -> usize {
        fn backing<T>(values: &Vec<T>) -> usize {
            values.capacity().saturating_mul(std::mem::size_of::<T>())
        }
        backing(&self.admissions)
            .saturating_add(backing(&self.capacities))
            .saturating_add(backing(&self.charges))
            .saturating_add(backing(&self.jobs))
            .saturating_add(self.job_checkouts.bytes())
            .saturating_add(backing(&self.signals))
            .saturating_add(self.signal_checkouts.bytes())
            .saturating_add(backing(&self.subscriptions))
            .saturating_add(backing(&self.portion))
            .saturating_add(backing(&self.parents))
            .saturating_add(backing(&self.finishes))
            .saturating_add(backing(&self.suppressed))
    }
}

struct Idle {
    entries: [Option<Buffers>; IDLE_COUNT],
    bytes: usize,
    retired: bool,
}

pub(super) struct ScratchPool {
    idle: Mutex<Idle>,
}

impl ScratchPool {
    pub(super) fn new() -> Self {
        Self {
            idle: Mutex::new(Idle {
                entries: std::array::from_fn(|_| None),
                bytes: 0,
                retired: false,
            }),
        }
    }

    pub(super) fn acquire(&self) -> Lease<'_> {
        let buffers = match self.idle.try_lock() {
            Ok(mut idle) => Self::take(&mut idle),
            Err(TryLockError::Poisoned(error)) => Self::take(&mut error.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        };
        Lease {
            pool: self,
            buffers: Some(buffers.unwrap_or_default()),
        }
    }

    fn take(idle: &mut Idle) -> Option<Buffers> {
        if idle.retired {
            return None;
        }
        let buffers = idle.entries.iter_mut().find_map(Option::take)?;
        idle.bytes -= buffers.bytes();
        Some(buffers)
    }

    pub(super) fn retire(&self) {
        let entries = {
            let mut idle = self.idle.lock().unwrap_or_else(|error| error.into_inner());
            idle.retired = true;
            idle.bytes = 0;
            std::mem::replace(&mut idle.entries, std::array::from_fn(|_| None))
        };
        drop(entries);
    }
}

pub(super) struct Lease<'a> {
    pool: &'a ScratchPool,
    buffers: Option<Buffers>,
}

impl Deref for Lease<'_> {
    type Target = Buffers;
    fn deref(&self) -> &Buffers {
        self.buffers.as_ref().expect("active scratch lease")
    }
}

impl DerefMut for Lease<'_> {
    fn deref_mut(&mut self) -> &mut Buffers {
        self.buffers.as_mut().expect("active scratch lease")
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let mut buffers = self.buffers.take().expect("active scratch lease");
        buffers.clear();
        let bytes = buffers.bytes();
        if bytes == 0 || bytes > IDLE_BYTES {
            return;
        }
        let mut idle = match self.pool.idle.try_lock() {
            Ok(idle) => idle,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return,
        };
        if !idle.retired
            && bytes <= IDLE_BYTES - idle.bytes
            && let Some(entry) = idle.entries.iter_mut().find(|entry| entry.is_none())
        {
            *entry = Some(buffers);
            idle.bytes += bytes;
        } else {
            // Excess backing and any future owning fields must drop unlocked.
            drop(idle);
            drop(buffers);
        }
    }
}
