//! Optional declared-cost admission for staged pipelines.
//!
//! This helper accounts promised capacity. Native owned-job limits still apply
//! to every submission; a reservation does not itself enqueue work.

use std::sync::{Arc, Mutex, OnceLock, Weak};

/// Simultaneously live scheduler metadata, promised deliveries, and payload bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SWCost {
    pub records: usize,
    pub edges: usize,
    pub deliveries: usize,
    pub bytes: usize,
}

impl SWCost {
    pub const fn new(records: usize, edges: usize, deliveries: usize, bytes: usize) -> Self {
        Self {
            records,
            edges,
            deliveries,
            bytes,
        }
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.records.checked_add(other.records)?,
            self.edges.checked_add(other.edges)?,
            self.deliveries.checked_add(other.deliveries)?,
            self.bytes.checked_add(other.bytes)?,
        ))
    }

    fn fits(self, limit: Self) -> bool {
        self.records <= limit.records
            && self.edges <= limit.edges
            && self.deliveries <= limit.deliveries
            && self.bytes <= limit.bytes
    }

    fn metadata_fits(self, limit: Self) -> bool {
        self.records <= limit.records
            && self.edges <= limit.edges
            && self.deliveries <= limit.deliveries
    }

    fn subtract(&mut self, other: Self) -> bool {
        if !other.fits(*self) {
            return false;
        }
        self.records -= other.records;
        self.edges -= other.edges;
        self.deliveries -= other.deliveries;
        self.bytes -= other.bytes;
        true
    }

    fn add(&mut self, other: Self) {
        *self = self
            .checked_add(other)
            .expect("reservation accounting overflow");
    }
}

/// Optional admission policy. Required work may exceed the ordinary byte target.
/// The required allowance's byte field is ignored: only an explicit hard ceiling
/// constrains aggregate required bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWLimits {
    pub ordinary_target: SWCost,
    pub required_allowance: SWCost,
    pub required_pipelines: usize,
    pub hard_byte_ceiling: Option<usize>,
}

impl SWLimits {
    pub fn new(
        ordinary_target: SWCost,
        required_allowance: SWCost,
        required_pipelines: usize,
        hard_byte_ceiling: Option<usize>,
    ) -> Result<Self, SWLimitError> {
        if required_pipelines > 0
            && required_allowance.records == 0
            && required_allowance.edges == 0
            && required_allowance.deliveries == 0
        {
            return Err(SWLimitError::ZeroRequiredCapacity);
        }
        if ordinary_target
            .records
            .checked_add(required_allowance.records)
            .is_none()
            || ordinary_target
                .edges
                .checked_add(required_allowance.edges)
                .is_none()
            || ordinary_target
                .deliveries
                .checked_add(required_allowance.deliveries)
                .is_none()
        {
            return Err(SWLimitError::MetadataOverflow);
        }
        if hard_byte_ceiling.is_some_and(|ceiling| ordinary_target.bytes > ceiling) {
            return Err(SWLimitError::OrdinaryTargetExceedsCeiling);
        }
        Ok(Self {
            ordinary_target,
            required_allowance,
            required_pipelines,
            hard_byte_ceiling,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWLimitError {
    ZeroRequiredCapacity,
    MetadataOverflow,
    OrdinaryTargetExceedsCeiling,
}

impl std::fmt::Display for SWLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRequiredCapacity => {
                f.write_str("required pipeline capacity needs protected metadata")
            }
            Self::MetadataOverflow => f.write_str("combined metadata capacity overflows usize"),
            Self::OrdinaryTargetExceedsCeiling => {
                f.write_str("ordinary byte target exceeds hard ceiling")
            }
        }
    }
}

impl std::error::Error for SWLimitError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWReservationError {
    Full,
    TooLarge,
    Closed,
    InsufficientCredits,
}

impl std::fmt::Display for SWReservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => f.write_str("reservation capacity is currently full"),
            Self::TooLarge => f.write_str("reservation cannot fit configured capacity"),
            Self::Closed => f.write_str("reservation admission is closed"),
            Self::InsufficientCredits => f.write_str("reservation has insufficient free credits"),
        }
    }
}

impl std::error::Error for SWReservationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Ordinary,
    Required,
}

#[derive(Default)]
struct Usage {
    ordinary: SWCost,
    required: SWCost,
    pipelines: usize,
    closed: bool,
}

struct Ledger {
    runtime_identity: u64,
    limits: SWLimits,
    usage: Mutex<Usage>,
    wake: OnceLock<Arc<crate::progress::SWWake>>,
}

/// Shared optional admission domain. Runtime owns this manager; callers receive
/// only reservations bound to its identity.
#[derive(Clone)]
pub(crate) struct SWReservationPool {
    ledger: Arc<Ledger>,
}

impl SWReservationPool {
    pub(crate) fn new(runtime_identity: u64, limits: SWLimits) -> Self {
        Self {
            ledger: Arc::new(Ledger {
                runtime_identity,
                limits,
                usage: Mutex::new(Usage::default()),
                wake: OnceLock::new(),
            }),
        }
    }

    pub(crate) fn set_wake(&self, wake: Arc<crate::progress::SWWake>) {
        let _ = self.ledger.wake.set(wake);
    }

    pub(crate) fn close(&self) {
        self.ledger
            .usage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed = true;
        self.ledger.notify();
    }

    pub(crate) fn required_delivery_capacity(&self) -> usize {
        self.ledger.limits.required_allowance.deliveries
    }

    /// Reserves an ordinary window without using protected metadata capacity.
    pub(crate) fn try_reserve_ordinary(
        &self,
        cost: SWCost,
    ) -> Result<SWReservation, SWReservationError> {
        self.reserve(cost, Kind::Ordinary)
    }

    /// Reserves one required pipeline and its simultaneous stage/window peak.
    pub(crate) fn try_reserve_required(
        &self,
        cost: SWCost,
    ) -> Result<SWReservation, SWReservationError> {
        self.reserve(cost, Kind::Required)
    }

    fn reserve(&self, cost: SWCost, kind: Kind) -> Result<SWReservation, SWReservationError> {
        self.ledger.claim(cost, kind, true)?;
        let pipeline = Arc::new(Pipeline {
            ledger: Arc::clone(&self.ledger),
            kind,
            total: Mutex::new(cost),
        });
        Ok(SWReservation {
            pipeline,
            credits: Arc::new(CreditBank {
                state: Mutex::new(CreditState {
                    balance: cost,
                    retired: false,
                }),
                parent: None,
            }),
        })
    }

    pub(crate) fn snapshot(&self) -> SWCapacityUsage {
        let usage = self.ledger.usage.lock().unwrap_or_else(|e| e.into_inner());
        SWCapacityUsage {
            ordinary: usage.ordinary,
            required: usage.required,
            required_pipelines: usage.pipelines,
        }
    }
}

/// Current charges, including credits parked in pipeline reservations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWCapacityUsage {
    pub ordinary: SWCost,
    pub required: SWCost,
    pub required_pipelines: usize,
}

impl Ledger {
    fn notify(&self) {
        if let Some(wake) = self.wake.get() {
            wake.notify();
        }
    }
    fn claim(
        &self,
        cost: SWCost,
        kind: Kind,
        new_pipeline: bool,
    ) -> Result<(), SWReservationError> {
        let limits = self.limits;
        let mut usage = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        if usage.closed {
            return Err(SWReservationError::Closed);
        }
        let allowance = match kind {
            Kind::Ordinary => limits.ordinary_target,
            Kind::Required => limits.required_allowance,
        };
        let individually_possible = match kind {
            Kind::Ordinary => cost.fits(allowance),
            Kind::Required => cost.metadata_fits(allowance),
        };
        if !individually_possible
            || limits
                .hard_byte_ceiling
                .is_some_and(|ceiling| cost.bytes > ceiling)
        {
            return Err(SWReservationError::TooLarge);
        }
        if new_pipeline && kind == Kind::Required && limits.required_pipelines == 0 {
            return Err(SWReservationError::TooLarge);
        }
        if new_pipeline && kind == Kind::Required && usage.pipelines >= limits.required_pipelines {
            return Err(SWReservationError::Full);
        }
        let current = match kind {
            Kind::Ordinary => usage.ordinary,
            Kind::Required => usage.required,
        };
        let Some(next) = current.checked_add(cost) else {
            return Err(SWReservationError::Full);
        };
        let available = match kind {
            Kind::Ordinary => next.fits(allowance),
            Kind::Required => next.metadata_fits(allowance),
        };
        let total_bytes = usage
            .ordinary
            .bytes
            .checked_add(usage.required.bytes)
            .and_then(|bytes| bytes.checked_add(cost.bytes));
        if !available
            || total_bytes.is_none_or(|bytes| {
                limits
                    .hard_byte_ceiling
                    .is_some_and(|ceiling| bytes > ceiling)
            })
        {
            return Err(SWReservationError::Full);
        }
        match kind {
            Kind::Ordinary => usage.ordinary = next,
            Kind::Required => usage.required = next,
        }
        if new_pipeline && kind == Kind::Required {
            usage.pipelines += 1;
        }
        drop(usage);
        self.notify();
        Ok(())
    }

    fn release(&self, cost: SWCost, kind: Kind, pipeline: bool) {
        let mut usage = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        let current = match kind {
            Kind::Ordinary => &mut usage.ordinary,
            Kind::Required => &mut usage.required,
        };
        assert!(current.subtract(cost), "reservation release exceeds charge");
        if pipeline && kind == Kind::Required {
            usage.pipelines -= 1;
        }
        drop(usage);
        self.notify();
    }
}

struct Pipeline {
    ledger: Arc<Ledger>,
    kind: Kind,
    total: Mutex<SWCost>,
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        let total = *self.total.lock().unwrap_or_else(|e| e.into_inner());
        self.ledger.release(total, self.kind, true);
    }
}

/// A movable capacity capability. Split stage credits return to their parent on
/// drop; a retained-byte child stays charged independently of execution cleanup.
pub struct SWReservation {
    pipeline: Arc<Pipeline>,
    credits: Arc<CreditBank>,
}

struct CreditBank {
    state: Mutex<CreditState>,
    parent: Option<Arc<CreditBank>>,
}

struct CreditState {
    balance: SWCost,
    retired: bool,
}

impl CreditBank {
    /// Retirement and refund share the bank lock. A refund either precedes
    /// retirement's transfer or passes through to the nearest live ancestor.
    /// Hold only one bank lock at a time; retained bytes may keep dead banks alive.
    fn refund(&self, cost: SWCost) {
        let mut bank = self;
        loop {
            let mut state = bank.state.lock().unwrap_or_else(|e| e.into_inner());
            if !state.retired {
                state.balance.add(cost);
                return;
            }
            drop(state);
            let Some(parent) = &bank.parent else {
                // No reservation can spend these credits. Pipeline::drop still
                // releases their global charge after its last live stage ends.
                return;
            };
            bank = parent;
        }
    }
}

impl Drop for SWReservation {
    fn drop(&mut self) {
        let balance = {
            let mut state = self.credits.state.lock().unwrap_or_else(|e| e.into_inner());
            state.retired = true;
            std::mem::take(&mut state.balance)
        };
        if let Some(parent) = &self.credits.parent {
            parent.refund(balance);
        }
        self.pipeline.ledger.notify();
    }
}

impl SWReservation {
    pub fn is_required(&self) -> bool {
        self.pipeline.kind == Kind::Required
    }

    pub fn runtime_identity(&self) -> u64 {
        self.pipeline.ledger.runtime_identity
    }

    pub fn available(&self) -> SWCost {
        self.credits
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .balance
    }

    /// Transfers capacity to a child stage without touching global admission.
    pub fn stage(&self, cost: SWCost) -> Result<Self, SWReservationError> {
        let mut credits = self.credits.state.lock().unwrap_or_else(|e| e.into_inner());
        if !credits.balance.subtract(cost) {
            return Err(SWReservationError::InsufficientCredits);
        }
        Ok(Self {
            pipeline: Arc::clone(&self.pipeline),
            credits: Arc::new(CreditBank {
                state: Mutex::new(CreditState {
                    balance: cost,
                    retired: false,
                }),
                parent: Some(Arc::clone(&self.credits)),
            }),
        })
    }

    pub fn split(&mut self, cost: SWCost) -> Result<Self, SWReservationError> {
        self.stage(cost)
    }

    /// Transfers capacity from the pool atomically. Failure leaves this handle unchanged.
    pub fn try_grow(&mut self, additional: SWCost) -> Result<(), SWReservationError> {
        // Ledger::claim publishes progress while this operation still owns the
        // pipeline total lock. Delay the host adapter until both locks retire.
        let _notification = self
            .pipeline
            .ledger
            .wake
            .get()
            .map(|wake| wake.notification_scope());
        let mut total = self
            .pipeline
            .total
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(next) = total.checked_add(additional) else {
            return Err(SWReservationError::TooLarge);
        };
        let limits = self.pipeline.ledger.limits;
        let fits = match self.pipeline.kind {
            Kind::Ordinary => next.fits(limits.ordinary_target),
            Kind::Required => next.metadata_fits(limits.required_allowance),
        };
        if !fits
            || limits
                .hard_byte_ceiling
                .is_some_and(|ceiling| next.bytes > ceiling)
        {
            return Err(SWReservationError::TooLarge);
        }
        self.pipeline
            .ledger
            .claim(additional, self.pipeline.kind, false)?;
        *total = next;
        self.credits.refund(additional);
        Ok(())
    }

    /// Detaches declared result bytes from a pipeline. The returned lease keeps
    /// the global byte charge but does not itself occupy a pipeline concurrency
    /// slot. If released while a pipeline handle lives, credits return to that
    /// reserve; otherwise the global byte charge ends on release or explicit
    /// transfer to an external budget.
    pub fn retain_bytes(&self, bytes: usize) -> Result<SWByteLease, SWReservationError> {
        let mut total = self
            .pipeline
            .total
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut credits = self.credits.state.lock().unwrap_or_else(|e| e.into_inner());
        let cost = SWCost::new(0, 0, 0, bytes);
        if !credits.balance.subtract(cost) {
            return Err(SWReservationError::InsufficientCredits);
        }
        assert!(
            total.subtract(cost),
            "retained bytes exceed pipeline charge"
        );
        Ok(SWByteLease {
            ledger: Arc::clone(&self.pipeline.ledger),
            pipeline: Arc::downgrade(&self.pipeline),
            credits: Arc::clone(&self.credits),
            kind: self.pipeline.kind,
            bytes,
        })
    }
}

/// Accounted result bytes independent of execution and pipeline closure.
pub struct SWByteLease {
    ledger: Arc<Ledger>,
    pipeline: Weak<Pipeline>,
    credits: Arc<CreditBank>,
    kind: Kind,
    bytes: usize,
}

impl SWByteLease {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for SWByteLease {
    fn drop(&mut self) {
        let cost = SWCost::new(0, 0, 0, self.bytes);
        if let Some(pipeline) = self.pipeline.upgrade() {
            pipeline
                .total
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .add(cost);
            self.credits.refund(cost);
            self.ledger.notify();
        } else {
            self.ledger.release(cost, self.kind, false);
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/reservation.rs"]
mod tests;
