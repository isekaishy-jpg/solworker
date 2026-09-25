//! Optional, bounded host notification routing.

use std::cell::Cell;
use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// Preallocated route and binding capacities for one runtime. Zero routes
/// disables notification; zero bindings permits routes but no persistent
/// source registrations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SWNotifyLimits {
    pub routes: usize,
    pub bindings: usize,
}

/// A notification management or registration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWNotifyError {
    Disabled,
    Full,
    Closed,
    ForeignSource,
    ForeignStamp,
    InvalidContext,
    Faulted,
    CounterExhausted,
}

impl std::fmt::Display for SWNotifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "notification {:?}", self)
    }
}

impl std::error::Error for SWNotifyError {}

/// A failed signal function is retained as route status, separate from job state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SWNotifyFault {
    Error(String),
    Panicked,
    CounterExhausted,
}

/// Failed route construction returns the uninvoked signal closure.
pub struct SWNotifyRejected<F> {
    pub reason: SWNotifyError,
    pub signal: F,
}

impl<F> SWNotifyRejected<F> {
    pub fn into_signal(self) -> F {
        self.signal
    }
}

impl<F> std::fmt::Debug for SWNotifyRejected<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SWNotifyRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

type SignalFn = dyn Fn() -> io::Result<()> + Send + Sync + 'static;

struct DomainState {
    free_routes: Vec<usize>,
    free_bindings: Vec<usize>,
}

/// Runtime-scoped notification storage. It retains no runtime workers.
pub(crate) struct NotifyDomain {
    runtime_id: u64,
    state: Mutex<DomainState>,
    routes: Vec<Arc<RouteCell>>,
    bindings: Vec<Arc<BindingCell>>,
    progress: OnceLock<NotifySource>,
    source_serial: Mutex<u64>,
}

impl NotifyDomain {
    pub(crate) fn new(runtime_id: u64, limits: SWNotifyLimits) -> Arc<Self> {
        let domain = Arc::new_cyclic(|weak| Self {
            runtime_id,
            state: Mutex::new(DomainState {
                free_routes: (0..limits.routes).rev().collect(),
                free_bindings: (0..limits.bindings).rev().collect(),
            }),
            routes: (0..limits.routes)
                .map(|index| Arc::new(RouteCell::new(index, weak.clone())))
                .collect(),
            bindings: (0..limits.bindings)
                .map(|index| Arc::new(BindingCell::new(index, weak.clone())))
                .collect(),
            progress: OnceLock::new(),
            source_serial: Mutex::new(0),
        });
        if limits.routes != 0 {
            let _ = domain.progress.set(NotifySource::new(&domain));
        }
        domain
    }

    pub(crate) fn progress_source(&self) -> NotifySource {
        self.progress
            .get()
            .expect("enabled notification domain has progress source")
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn with_progress_source_lock_for_test(&self, callback: impl FnOnce()) {
        let source = self.progress_source();
        let _guard = lock(&source.inner.state);
        callback();
    }

    pub(crate) fn create_route<F>(
        self: &Arc<Self>,
        signal: F,
    ) -> Result<SWNotifyRoute, SWNotifyRejected<F>>
    where
        F: Fn() -> io::Result<()> + Send + Sync + 'static,
    {
        if invocation_active() {
            return Err(SWNotifyRejected {
                reason: SWNotifyError::InvalidContext,
                signal,
            });
        }
        if self.routes.is_empty() {
            return Err(SWNotifyRejected {
                reason: SWNotifyError::Disabled,
                signal,
            });
        }
        let Some(index) = lock(&self.state).free_routes.pop() else {
            return Err(SWNotifyRejected {
                reason: SWNotifyError::Full,
                signal,
            });
        };
        let cell = Arc::clone(&self.routes[index]);
        let mut state = lock(&cell.state);
        let Some(generation) = state.generation.checked_add(1) else {
            lock(&self.state).free_routes.push(index);
            return Err(SWNotifyRejected {
                reason: SWNotifyError::CounterExhausted,
                signal,
            });
        };
        state.generation = generation;
        state.occupied = true;
        state.handle_alive = true;
        state.closed = false;
        state.pending = false;
        state.queued = false;
        state.epoch = 0;
        state.claims = 0;
        state.fault = None;
        state.signal = Some(Arc::new(signal));
        drop(state);
        Ok(SWNotifyRoute {
            cell,
            generation,
            domain: Arc::clone(self),
            _not_sync: PhantomData,
        })
    }

    fn reserve_binding(self: &Arc<Self>) -> Result<(Arc<BindingCell>, u64), SWNotifyError> {
        let Some(index) = lock(&self.state).free_bindings.pop() else {
            return Err(SWNotifyError::Full);
        };
        let cell = Arc::clone(&self.bindings[index]);
        let mut state = lock(&cell.state);
        let Some(generation) = state.generation.checked_add(1) else {
            lock(&self.state).free_bindings.push(index);
            return Err(SWNotifyError::CounterExhausted);
        };
        state.generation = generation;
        state.occupied = true;
        state.active = false;
        state.source = Weak::new();
        state.route = Weak::new();
        state.route_generation = 0;
        state.next = None;
        drop(state);
        Ok((cell, generation))
    }

    fn return_binding(&self, cell: &BindingCell, generation: u64) {
        let mut state = lock(&cell.state);
        if state.generation != generation || !state.occupied {
            return;
        }
        state.active = false;
        state.occupied = false;
        state.source = Weak::new();
        state.route = Weak::new();
        state.next = None;
        drop(state);
        lock(&self.state).free_bindings.push(cell.index);
    }

    fn maybe_return_route(&self, cell: &RouteCell, generation: u64) {
        let signal = {
            let mut state = lock(&cell.state);
            if state.generation != generation
                || !state.occupied
                || state.handle_alive
                || state.claims != 0
            {
                return;
            }
            state.occupied = false;
            state.signal.take()
        };
        if let Some(signal) = signal {
            let _invocation = InvocationGuard::enter();
            discard_value(signal);
        }
        lock(&self.state).free_routes.push(cell.index);
    }
}

struct SourceState {
    terminal: bool,
    head: Option<Arc<BindingCell>>,
}

struct SourceInner {
    runtime_id: u64,
    serial: u64,
    ever_watched: AtomicBool,
    #[cfg(test)]
    source_lock_entries: AtomicUsize,
    state: Mutex<SourceState>,
}

/// A direct, source-local publication head. Cloning retains this exact source.
#[derive(Clone)]
pub(crate) struct NotifySource {
    inner: Arc<SourceInner>,
}

impl std::fmt::Debug for NotifySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotifySource")
            .field("runtime_id", &self.inner.runtime_id)
            .field("serial", &self.inner.serial)
            .finish()
    }
}

impl NotifySource {
    pub(crate) fn new(domain: &Arc<NotifyDomain>) -> Self {
        let mut serial = lock(&domain.source_serial);
        *serial = serial
            .checked_add(1)
            .expect("notification source serial exhausted");
        Self {
            inner: Arc::new(SourceInner {
                runtime_id: domain.runtime_id,
                serial: *serial,
                ever_watched: AtomicBool::new(false),
                #[cfg(test)]
                source_lock_entries: AtomicUsize::new(0),
                state: Mutex::new(SourceState {
                    terminal: false,
                    head: None,
                }),
            }),
        }
    }

    pub(crate) fn runtime_id(&self) -> u64 {
        self.inner.runtime_id
    }

    pub(crate) fn serial(&self) -> u64 {
        self.inner.serial
    }

    /// Marks a persistent source changed. Adapter calls drain at scope exit.
    pub(crate) fn publish(&self) {
        self.publish_impl(false);
    }

    /// Broad nonterminal progress can skip source bookkeeping until its first
    /// successful registration. The gate stays enabled after detachment.
    pub(crate) fn publish_if_watched(&self) {
        if self.inner.ever_watched.load(Ordering::SeqCst) {
            self.publish_impl(false);
        }
    }

    /// Marks a one-shot source changed and removes its memberships.
    pub(crate) fn publish_terminal(&self) {
        self.publish_impl(true);
    }

    fn publish_impl(&self, terminal: bool) {
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::record(
            "notify.publish.begin",
            self.runtime_id(),
            self.serial(),
            u64::from(terminal),
        );
        let _scope = NotificationScope::enter();
        #[cfg(test)]
        self.inner
            .source_lock_entries
            .fetch_add(1, Ordering::SeqCst);
        let mut state = lock(&self.inner.state);
        if state.terminal {
            return;
        }
        if terminal {
            state.terminal = true;
        }
        let mut current = state.head.clone();
        while let Some(cell) = current {
            let mut binding = lock(&cell.state);
            current = binding.next.clone();
            if !binding.active {
                continue;
            }
            if let Some(route) = binding.route.upgrade() {
                route.mark_changed(binding.route_generation);
            }
            if terminal {
                binding.active = false;
                binding.source = Weak::new();
                binding.route = Weak::new();
                binding.next = None;
            }
        }
        if terminal {
            state.head = None;
        }
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::record(
            "notify.publish.marked",
            self.runtime_id(),
            self.serial(),
            u64::from(terminal),
        );
    }
}

struct BindingState {
    generation: u64,
    occupied: bool,
    active: bool,
    source: Weak<SourceInner>,
    route: Weak<RouteCell>,
    route_generation: u64,
    next: Option<Arc<BindingCell>>,
}

struct BindingCell {
    index: usize,
    _domain: Weak<NotifyDomain>,
    state: Mutex<BindingState>,
}

impl BindingCell {
    fn new(index: usize, domain: Weak<NotifyDomain>) -> Self {
        Self {
            index,
            _domain: domain,
            state: Mutex::new(BindingState {
                generation: 0,
                occupied: false,
                active: false,
                source: Weak::new(),
                route: Weak::new(),
                route_generation: 0,
                next: None,
            }),
        }
    }

    fn detach(&self, generation: u64) {
        let source = {
            let state = lock(&self.state);
            if state.generation != generation || !state.active {
                return;
            }
            state.source.upgrade()
        };
        if let Some(source) = source {
            self.detach_from_source(generation, &source);
        } else {
            let mut state = lock(&self.state);
            if state.generation == generation {
                state.active = false;
                state.route = Weak::new();
                state.next = None;
            }
        }
    }

    fn detach_from_source(&self, generation: u64, source: &Arc<SourceInner>) {
        let mut source_state = lock(&source.state);
        {
            let state = lock(&self.state);
            if state.generation != generation
                || !state.active
                || !Weak::ptr_eq(&state.source, &Arc::downgrade(source))
            {
                return;
            }
        }
        let mut previous: Option<Arc<BindingCell>> = None;
        let mut current = source_state.head.clone();
        while let Some(cell) = current {
            let next = lock(&cell.state).next.clone();
            if cell.index == self.index {
                debug_assert_eq!(lock(&self.state).generation, generation);
                if let Some(previous) = previous {
                    lock(&previous.state).next = next.clone();
                } else {
                    source_state.head = next.clone();
                }
                let mut state = lock(&self.state);
                if state.generation == generation {
                    state.active = false;
                    state.source = Weak::new();
                    state.route = Weak::new();
                    state.next = None;
                }
                break;
            }
            previous = Some(cell);
            current = next;
        }
    }
}

struct RouteState {
    generation: u64,
    occupied: bool,
    handle_alive: bool,
    closed: bool,
    pending: bool,
    queued: bool,
    epoch: u64,
    claims: usize,
    fault: Option<SWNotifyFault>,
    signal: Option<Arc<SignalFn>>,
}

struct RouteCell {
    index: usize,
    domain: Weak<NotifyDomain>,
    state: Mutex<RouteState>,
    next: Mutex<Option<Arc<RouteCell>>>,
}

impl RouteCell {
    #[cfg(feature = "diagnostics")]
    fn trace(&self, event: &'static str, related: u64) {
        if let Some(domain) = self.domain.upgrade() {
            crate::diagnostics::record(event, domain.runtime_id, self.index as u64 + 1, related);
        }
    }

    fn new(index: usize, domain: Weak<NotifyDomain>) -> Self {
        Self {
            index,
            domain,
            state: Mutex::new(RouteState {
                generation: 0,
                occupied: false,
                handle_alive: false,
                closed: true,
                pending: false,
                queued: false,
                epoch: 0,
                claims: 0,
                fault: None,
                signal: None,
            }),
            next: Mutex::new(None),
        }
    }

    fn mark_changed(self: &Arc<Self>, generation: u64) {
        let queue = {
            let mut state = lock(&self.state);
            if state.generation != generation || state.closed || state.fault.is_some() {
                return;
            }
            let Some(epoch) = state.epoch.checked_add(1) else {
                state.fault = Some(SWNotifyFault::CounterExhausted);
                return;
            };
            state.epoch = epoch;
            #[cfg(feature = "diagnostics")]
            self.trace("notify.changed", epoch);
            if state.pending {
                #[cfg(feature = "diagnostics")]
                self.trace("notify.coalesced", epoch);
                return;
            }
            state.pending = true;
            if state.queued {
                false
            } else {
                state.queued = true;
                state.claims += 1;
                true
            }
        };
        if queue {
            queue_claim(Arc::clone(self));
        }
    }

    fn run_claim(self: &Arc<Self>) {
        let _invocation = InvocationGuard::enter();
        let signal = {
            let mut state = lock(&self.state);
            state.queued = false;
            state.signal.as_ref().map(Arc::clone)
        };
        if let Some(signal) = signal {
            #[cfg(feature = "diagnostics")]
            self.trace("notify.signal.begin", 0);
            let result = catch_unwind(AssertUnwindSafe(|| signal()));
            #[cfg(feature = "diagnostics")]
            self.trace("notify.signal.end", 0);
            let mut fault = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => {
                    let rendered = catch_unwind(AssertUnwindSafe(|| error.to_string()));
                    let drop_panicked = discard_value(error);
                    match rendered {
                        Ok(message) if !drop_panicked => Some(SWNotifyFault::Error(message)),
                        Ok(_) => Some(SWNotifyFault::Panicked),
                        Err(payload) => {
                            crate::cleanup::discard_panic(payload);
                            Some(SWNotifyFault::Panicked)
                        }
                    }
                }
                Err(payload) => {
                    crate::cleanup::discard_panic(payload);
                    Some(SWNotifyFault::Panicked)
                }
            };
            if discard_value(signal) {
                fault = Some(SWNotifyFault::Panicked);
            }
            let mut state = lock(&self.state);
            if state.fault.is_none() {
                state.fault = fault;
            }
        }
        let (released, generation) = {
            let mut state = lock(&self.state);
            state.claims -= 1;
            let signal = if state.closed && state.claims == 0 {
                state.signal.take()
            } else {
                None
            };
            // Keep a retirement claim until user capture destruction completes.
            // Otherwise a dropped controller could recycle this cell while its
            // last signal reference is still running arbitrary destructor code.
            if signal.is_some() {
                state.claims = 1;
            }
            (signal, state.generation)
        };
        let retiring = released.is_some();
        let panicked = released.is_some_and(discard_value);
        if retiring {
            let mut state = lock(&self.state);
            if panicked && state.fault.is_none() {
                state.fault = Some(SWNotifyFault::Panicked);
            }
            state.claims -= 1;
        }
        if let Some(domain) = self.domain.upgrade() {
            domain.maybe_return_route(self, generation);
        }
    }
}

/// Opaque route epoch from `prepare_wait`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWNotifyStamp {
    runtime_id: u64,
    route_index: usize,
    generation: u64,
    epoch: u64,
}

/// A single host destination controller. It is transferable, but has one arm
/// owner. Signal closures run synchronously on publishers outside solworker
/// publication locks and may overlap after successive arms. They must only
/// signal a durable host wake destination; they must not run work or wait.
pub struct SWNotifyRoute {
    cell: Arc<RouteCell>,
    generation: u64,
    domain: Arc<NotifyDomain>,
    _not_sync: PhantomData<Cell<()>>,
}

impl SWNotifyRoute {
    pub(crate) fn watch_source(
        &mut self,
        source: &NotifySource,
    ) -> Result<SWNotifyBinding, SWNotifyError> {
        if invocation_active() {
            return Err(SWNotifyError::InvalidContext);
        }
        debug_assert_ne!(source.serial(), 0);
        if source.runtime_id() != self.domain.runtime_id {
            return Err(SWNotifyError::ForeignSource);
        }
        {
            let route = lock(&self.cell.state);
            if route.closed {
                return Err(SWNotifyError::Closed);
            }
            if route.fault.is_some() {
                return Err(SWNotifyError::Faulted);
            }
        }
        let (cell, generation) = self.domain.reserve_binding()?;
        let mut source_state = lock(&source.inner.state);
        let mut binding = lock(&cell.state);
        let route = lock(&self.cell.state);
        let error = if route.generation != self.generation || route.closed {
            Some(SWNotifyError::Closed)
        } else if route.fault.is_some() {
            Some(SWNotifyError::Faulted)
        } else {
            None
        };
        if let Some(error) = error {
            drop(route);
            drop(binding);
            drop(source_state);
            self.domain.return_binding(&cell, generation);
            return Err(error);
        }
        if !source_state.terminal {
            binding.active = true;
            binding.source = Arc::downgrade(&source.inner);
            binding.route = Arc::downgrade(&self.cell);
            binding.route_generation = self.generation;
            binding.next = source_state.head.take();
            source_state.head = Some(Arc::clone(&cell));
            // Membership is installed under the source lock before a publisher
            // can observe this flag. The initial mark below covers a publisher
            // whose false load preceded this first registration.
            source.inner.ever_watched.store(true, Ordering::SeqCst);
        }
        drop(route);
        drop(binding);
        drop(source_state);
        #[cfg(feature = "diagnostics")]
        self.cell.trace("notify.watch", source.serial());
        // Initial recheck covers state published before or during registration.
        self.cell.mark_changed(self.generation);
        Ok(SWNotifyBinding {
            cell: Some(cell),
            generation,
            domain: Some(Arc::clone(&self.domain)),
        })
    }

    /// Watches this runtime's broad scheduler and control progress.
    pub fn watch_progress(&mut self) -> Result<SWNotifyBinding, SWNotifyError> {
        let source = self.domain.progress_source();
        self.watch_source(&source)
    }

    /// Watches one terminal completion token. Foreign-runtime tokens are
    /// rejected even after completion. Runtime-independent ready tokens request
    /// an immediate recheck and return an inert binding.
    pub fn watch_completion(
        &mut self,
        completion: &crate::task::SWCompletion,
    ) -> Result<SWNotifyBinding, SWNotifyError> {
        if invocation_active() {
            return Err(SWNotifyError::InvalidContext);
        }
        if let Some(runtime_id) = completion.notification_runtime_id()
            && runtime_id != self.domain.runtime_id
        {
            return Err(SWNotifyError::ForeignSource);
        }
        if let Some(source) = completion.notification_source() {
            return self.watch_source(&source);
        }
        if completion.status().is_some() && completion.notification_runtime_id().is_none() {
            let state = lock(&self.cell.state);
            if state.closed {
                return Err(SWNotifyError::Closed);
            }
            if state.fault.is_some() {
                return Err(SWNotifyError::Faulted);
            }
            drop(state);
            self.cell.mark_changed(self.generation);
            return Ok(SWNotifyBinding::inert());
        }
        Err(SWNotifyError::Closed)
    }

    /// Watches one owner's delivery and lifecycle progress.
    pub fn watch_owner<O>(
        &mut self,
        owner: &crate::owner::SWOwner<O>,
    ) -> Result<SWNotifyBinding, SWNotifyError> {
        let source = owner.notification_source().ok_or(SWNotifyError::Disabled)?;
        self.watch_source(&source)
    }

    /// Clears pending and returns a stamp for the subsequent state recheck.
    /// Drain the host wake primitive before this call, then inspect source
    /// state and use `changed_since` before entering the host wait.
    pub fn prepare_wait(&mut self) -> Result<SWNotifyStamp, SWNotifyError> {
        if invocation_active() {
            return Err(SWNotifyError::InvalidContext);
        }
        let mut state = lock(&self.cell.state);
        if state.closed {
            return Err(SWNotifyError::Closed);
        }
        if state.fault.is_some() {
            return Err(SWNotifyError::Faulted);
        }
        state.pending = false;
        #[cfg(feature = "diagnostics")]
        self.cell.trace("notify.arm", state.epoch);
        Ok(SWNotifyStamp {
            runtime_id: self.domain.runtime_id,
            route_index: self.cell.index,
            generation: self.generation,
            epoch: state.epoch,
        })
    }

    /// Checks changes and lifecycle after inspecting authoritative source state.
    pub fn changed_since(&self, stamp: SWNotifyStamp) -> Result<bool, SWNotifyError> {
        if invocation_active() {
            return Err(SWNotifyError::InvalidContext);
        }
        if stamp.runtime_id != self.domain.runtime_id
            || stamp.route_index != self.cell.index
            || stamp.generation != self.generation
        {
            return Err(SWNotifyError::ForeignStamp);
        }
        let state = lock(&self.cell.state);
        let changed = state.closed || state.fault.is_some() || state.epoch != stamp.epoch;
        #[cfg(feature = "diagnostics")]
        self.cell.trace(
            if changed {
                "notify.recheck.changed"
            } else {
                "notify.recheck.unchanged"
            },
            stamp.epoch,
        );
        Ok(changed)
    }

    pub fn fault(&self) -> Option<SWNotifyFault> {
        lock(&self.cell.state).fault.clone()
    }

    /// Prevents new claims and detaches interests without waiting. Claimed
    /// callbacks may remain; poll `is_quiescent` before releasing host resources.
    pub fn close(&mut self) -> Result<(), SWNotifyError> {
        if invocation_active() {
            return Err(SWNotifyError::InvalidContext);
        }
        self.close_inner();
        Ok(())
    }

    fn close_inner(&mut self) {
        let released = {
            let mut state = lock(&self.cell.state);
            if state.closed {
                return;
            }
            state.closed = true;
            if state.claims == 0 {
                state.signal.take()
            } else {
                None
            }
        };
        for cell in &self.domain.bindings {
            let generation = {
                let state = lock(&cell.state);
                if !state.active || state.route_generation != self.generation {
                    continue;
                }
                let Some(route) = state.route.upgrade() else {
                    continue;
                };
                if !Arc::ptr_eq(&route, &self.cell) {
                    continue;
                }
                state.generation
            };
            cell.detach(generation);
        }
        if let Some(released) = released {
            let _invocation = InvocationGuard::enter();
            if discard_value(released) {
                let mut state = lock(&self.cell.state);
                if state.fault.is_none() {
                    state.fault = Some(SWNotifyFault::Panicked);
                }
            }
        }
    }

    /// True after close and all queued or running claims retire.
    pub fn is_quiescent(&self) -> bool {
        let state = lock(&self.cell.state);
        state.closed && state.claims == 0
    }
}

impl Drop for SWNotifyRoute {
    fn drop(&mut self) {
        self.close_inner();
        let generation = {
            let mut state = lock(&self.cell.state);
            state.handle_alive = false;
            state.generation
        };
        self.domain.maybe_return_route(&self.cell, generation);
    }
}

/// RAII source registration. Dropping it removes interest without waiting.
pub struct SWNotifyBinding {
    cell: Option<Arc<BindingCell>>,
    generation: u64,
    domain: Option<Arc<NotifyDomain>>,
}

impl SWNotifyBinding {
    fn inert() -> Self {
        Self {
            cell: None,
            generation: 0,
            domain: None,
        }
    }
}

impl Drop for SWNotifyBinding {
    fn drop(&mut self) {
        if let (Some(cell), Some(domain)) = (self.cell.take(), self.domain.take()) {
            cell.detach(self.generation);
            domain.return_binding(&cell, self.generation);
        }
    }
}

struct ThreadQueue {
    depth: usize,
    draining: bool,
    invocation: usize,
    head: Option<Arc<RouteCell>>,
    tail: Option<Arc<RouteCell>>,
}

thread_local! {
    // Host TLS destructors can publish and release routes after ordinary TLS
    // destruction has started. Do not register a destructor for this slot.
    // Scopes drain every owned link before returning, including on unwind;
    // the idle state retains no cells or heap storage to clean up at exit.
    static QUEUE: ManuallyDrop<RefCell<ThreadQueue>> = const { ManuallyDrop::new(RefCell::new(ThreadQueue {
        depth: 0,
        draining: false,
        invocation: 0,
        head: None,
        tail: None,
    })) };
}

/// Defers host adapters until the enclosing notification publication scope ends.
pub(crate) struct NotificationScope {
    entered: bool,
    // Enter and drop must update the same thread's scope depth.
    _not_send: PhantomData<Rc<()>>,
}

impl NotificationScope {
    pub(crate) fn enter() -> Self {
        Self::enter_if(true)
    }

    pub(crate) fn enter_if(enabled: bool) -> Self {
        if enabled {
            QUEUE.with(|queue| queue.borrow_mut().depth += 1);
        }
        Self {
            entered: enabled,
            _not_send: PhantomData,
        }
    }
}

impl Drop for NotificationScope {
    fn drop(&mut self) {
        if !self.entered {
            return;
        }
        let drain = QUEUE.with(|queue| {
            let mut queue = queue.borrow_mut();
            queue.depth -= 1;
            queue.depth == 0 && !queue.draining
        });
        if drain {
            drain_claims();
        }
    }
}

pub(crate) fn invocation_active() -> bool {
    QUEUE.with(|queue| queue.borrow().invocation != 0)
}

struct InvocationGuard;

impl InvocationGuard {
    fn enter() -> Self {
        QUEUE.with(|queue| queue.borrow_mut().invocation += 1);
        Self
    }
}

impl Drop for InvocationGuard {
    fn drop(&mut self) {
        QUEUE.with(|queue| queue.borrow_mut().invocation -= 1);
    }
}

fn queue_claim(cell: Arc<RouteCell>) {
    let drain = QUEUE.with(|queue| {
        let mut queue = queue.borrow_mut();
        if let Some(tail) = queue.tail.replace(Arc::clone(&cell)) {
            *lock(&tail.next) = Some(cell);
        } else {
            queue.head = Some(cell);
        }
        queue.depth == 0 && !queue.draining
    });
    if drain {
        drain_claims();
    }
}

fn drain_claims() {
    QUEUE.with(|queue| queue.borrow_mut().draining = true);
    loop {
        let cell = QUEUE.with(|queue| {
            let mut queue = queue.borrow_mut();
            let cell = queue.head.take()?;
            queue.head = lock(&cell.next).take();
            if queue.head.is_none() {
                queue.tail = None;
            }
            Some(cell)
        });
        let Some(cell) = cell else {
            break;
        };
        cell.run_claim();
    }
    QUEUE.with(|queue| queue.borrow_mut().draining = false);
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

/// User captures may run destructors on the final signal reference.
fn discard_value<T>(value: T) -> bool {
    match catch_unwind(AssertUnwindSafe(|| drop(value))) {
        Ok(()) => false,
        Err(payload) => {
            crate::cleanup::discard_panic(payload);
            true
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/notification.rs"]
mod tests;
