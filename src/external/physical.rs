//! Owned resources retained across access by an external provider or device.

use super::PhysicalRetention;
use crate::scheduler::SWSpawnError;
use crate::scheduler::reservation::SWByteLease;
use crate::task::SWRetained;
use std::sync::{Arc, Mutex};

/// An admitted resource that has not been exposed to a foreign accessor.
///
/// The provider may return it safely if submission fails before access starts.
/// Activation is the explicit boundary after which release needs proof from the
/// provider. `T` must be transferable because final cleanup may run on another
/// thread after the originating runtime has gone away.
pub struct SWExternalPrepared<T: Send + 'static> {
    resource: Box<T>,
    bytes: Option<SWByteLease>,
    retention: PhysicalRetention,
}

impl<T: Send + 'static> SWExternalPrepared<T> {
    pub(crate) fn new(
        resource: T,
        bytes: Option<SWByteLease>,
        retention: PhysicalRetention,
    ) -> Self {
        Self {
            resource: Box::new(resource),
            bytes,
            retention,
        }
    }

    /// Borrows the resource while no foreign access is permitted.
    pub fn get(&self) -> &T {
        &self.resource
    }

    /// Mutates the resource while no foreign access is permitted.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.resource
    }

    /// Transfers a resource that was never activated into ordinary retained
    /// ownership. Its declared byte charge follows the returned value.
    pub fn into_retained(self) -> SWRetained<T> {
        let Self {
            resource,
            bytes,
            retention,
        } = self;
        let retained = SWRetained::new(*resource, bytes);
        drop(retention);
        retained
    }

    /// Registers active physical access and fixes the resource's address.
    /// Rejection returns the complete prepared resource to the caller.
    pub fn activate(self) -> Result<SWExternalAccess<T>, SWExternalActivationRejected<T>> {
        if let Err(reason) = self.retention.activate() {
            return Err(SWExternalActivationRejected {
                reason,
                prepared: self,
            });
        }

        let inner = Arc::new(PhysicalCapsule {
            state: Mutex::new(PhysicalState {
                resource: Some(self.resource),
                bytes: self.bytes,
                retention: Some(Arc::new(self.retention)),
                anchor: None,
            }),
        });
        // The capsule owns one strong reference to itself before the ticket can
        // expose an address. Losing the ticket cannot free a foreign live region.
        inner.state.lock().unwrap_or_else(|e| e.into_inner()).anchor = Some(Arc::clone(&inner));
        Ok(SWExternalAccess { inner: Some(inner) })
    }
}

/// Activation rejection with the original, still unexposed resource.
pub struct SWExternalActivationRejected<T: Send + 'static> {
    pub reason: SWSpawnError,
    pub prepared: SWExternalPrepared<T>,
}

impl<T: Send + 'static> std::fmt::Debug for SWExternalActivationRejected<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SWExternalActivationRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

/// Exclusive release authority for a physically active external resource.
///
/// Dropping this ticket records an orphan and retains its resource, byte charge,
/// and physical registration. A provider must retain the ticket until it can
/// prove all foreign access has ended, including access by a device or callbacks.
/// Logical completion, cancellation, and timeout are not release proofs.
pub struct SWExternalAccess<T: Send + 'static> {
    inner: Option<Arc<PhysicalCapsule<T>>>,
}

struct PhysicalCapsule<T: Send + 'static> {
    state: Mutex<PhysicalState<T>>,
}

struct PhysicalState<T: Send + 'static> {
    resource: Option<Box<T>>,
    bytes: Option<SWByteLease>,
    retention: Option<Arc<PhysicalRetention>>,
    anchor: Option<Arc<PhysicalCapsule<T>>>,
}

impl<T: Send + 'static> SWExternalAccess<T> {
    /// Returns the stable address of the active resource for an external adapter.
    ///
    /// # Safety
    ///
    /// The adapter must prevent moves, reallocation, and conflicting aliases of
    /// every foreign-accessed region of `T`. The pointer must only be used while
    /// this physical operation remains active. In particular, a `Box<T>` fixes
    /// the address of `T` but does not fix allocations referenced by `T`.
    /// Repeated pointer requests must not create conflicting Rust aliases while
    /// the provider accesses the resource.
    pub unsafe fn as_mut_ptr(&self) -> *mut T {
        let inner = self.inner.as_ref().expect("active external ticket");
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .resource
            .as_deref_mut()
            .expect("active external resource") as *mut T
    }

    /// Transfers the resource and its declared byte charge into CPU-visible
    /// retained ownership after physical release has been proved.
    ///
    /// # Safety
    ///
    /// Every foreign or device accessor, including callbacks and queued uses,
    /// must have ended. The provider must establish all memory visibility needed
    /// before Rust reads or destroys `T`. A failure, cancellation, timeout, or
    /// unrelated completion counter does not establish this condition.
    pub unsafe fn acknowledge_release(mut self) -> SWRetained<T> {
        let inner = self.inner.take().expect("active external ticket");
        let (resource, bytes, retention, anchor) = inner.take_release();
        let retained = SWRetained::new(*resource, bytes);
        // The physical count retires only after the retained handoff exists.
        drop(retention);
        drop(anchor);
        retained
    }

    /// Destroys the resource after physical release has been proved.
    ///
    /// # Safety
    ///
    /// The same obligations as [`Self::acknowledge_release`] apply. The resource
    /// destructor must be legal on this thread; thread-affine destruction belongs
    /// in a provider-owned service instead.
    pub unsafe fn release(mut self) {
        let inner = self.inner.take().expect("active external ticket");
        let (resource, bytes, retention, anchor) = inner.take_release();
        // A user destructor can panic. Finish the accounting transition only
        // after its stack has unwound, then resume the original panic.
        let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(resource)));
        drop(bytes);
        drop(retention);
        drop(anchor);
        if let Err(panic) = cleanup {
            std::panic::resume_unwind(panic);
        }
    }
}

impl<T: Send + 'static> Drop for SWExternalAccess<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
            let retention = state.retention.as_ref().map(Arc::clone);
            drop(state);
            if let Some(retention) = retention {
                retention.mark_orphaned();
            }
            // The capsule's self-anchor intentionally survives ticket loss.
        }
    }
}

impl<T: Send + 'static> PhysicalCapsule<T> {
    fn take_release(
        &self,
    ) -> (
        Box<T>,
        Option<SWByteLease>,
        Arc<PhysicalRetention>,
        Arc<PhysicalCapsule<T>>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        (
            state.resource.take().expect("active external resource"),
            state.bytes.take(),
            state
                .retention
                .take()
                .expect("active physical registration"),
            state.anchor.take().expect("active physical anchor"),
        )
    }
}
