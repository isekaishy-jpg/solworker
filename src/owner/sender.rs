//! Transferable, phase-addressed callback submission.
//!
//! The inbox stores only `Send` callbacks. The owner takes and destroys them
//! on its own thread; sender handles never contain or access owner state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::delivery::{SWDelivery, SWDeliveryControl, Transport};
use super::{SWOwnerError, SWOwnerRejected, SWPhase};

pub(super) type SendCallback<O> = Box<dyn FnOnce(&mut O) + Send + 'static>;

struct InboxState<O> {
    closed: bool,
    faulted: bool,
    callbacks: HashMap<u64, (SWPhase, SendCallback<O>)>,
}

/// Transferable callback staging; only the owner may take a callback out.
pub(super) struct Inbox<O> {
    transport: Arc<Transport>,
    state: Mutex<InboxState<O>>,
}

impl<O> Inbox<O> {
    pub(super) fn new(transport: Arc<Transport>) -> Arc<Self> {
        Arc::new(Self {
            transport,
            state: Mutex::new(InboxState {
                closed: false,
                faulted: false,
                callbacks: HashMap::new(),
            }),
        })
    }

    pub(super) fn sender(self: &Arc<Self>) -> SWOwnerSender<O> {
        SWOwnerSender {
            inbox: Arc::clone(self),
        }
    }

    pub(super) fn take(&self, id: u64) -> Option<(SWPhase, SendCallback<O>)> {
        self.state.lock().unwrap().callbacks.remove(&id)
    }

    /// Close sender admission atomically with transport closure while keeping
    /// accepted packages for budgeted owner-thread cleanup.
    pub(super) fn mark_closed(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.transport.close();
    }

    pub(super) fn is_empty(&self) -> bool {
        self.state.lock().unwrap().callbacks.is_empty()
    }

    pub(super) fn set_faulted(&self, faulted: bool) {
        self.state.lock().unwrap().faulted = faulted;
    }

    /// Close admission and move captures out before any local destructor runs.
    pub(super) fn close_and_take(&self) -> Vec<(u64, SWPhase, SendCallback<O>)> {
        let callbacks = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            std::mem::take(&mut state.callbacks)
        };
        callbacks
            .into_iter()
            .map(|(id, (phase, callback))| (id, phase, callback))
            .collect()
    }
}

/// A cloneable sender for callbacks that may originate on other threads.
/// The callback is always deferred to an eligible owner phase.
pub struct SWOwnerSender<O> {
    inbox: Arc<Inbox<O>>,
}

impl<O> Clone for SWOwnerSender<O> {
    fn clone(&self) -> Self {
        Self {
            inbox: Arc::clone(&self.inbox),
        }
    }
}

impl<O> SWOwnerSender<O> {
    /// Reserves delivery before accepting the callback. Rejection returns it
    /// uninvoked, so the caller decides where its captures are destroyed.
    pub fn try_post<F>(
        &self,
        phase: SWPhase,
        callback: F,
    ) -> Result<(SWDelivery, SWDeliveryControl), SWOwnerRejected<F>>
    where
        F: FnOnce(&mut O) + Send + 'static,
    {
        let mut state = self.inbox.state.lock().unwrap();
        if state.closed || self.inbox.transport.is_closed() {
            return Err(SWOwnerRejected {
                reason: SWOwnerError::Closed,
                callback,
            });
        }
        if state.faulted {
            return Err(SWOwnerRejected {
                reason: SWOwnerError::Faulted,
                callback,
            });
        }
        let Some(reservation) = self.inbox.transport.reserve() else {
            return Err(SWOwnerRejected {
                reason: if self.inbox.transport.is_closed() {
                    SWOwnerError::Closed
                } else {
                    SWOwnerError::Full
                },
                callback,
            });
        };
        let id = reservation.id();
        let observer = reservation.observer();
        let control = reservation.control();
        state.callbacks.insert(id, (phase, Box::new(callback)));
        reservation.notifier().ready();
        Ok((observer, control))
    }
}
