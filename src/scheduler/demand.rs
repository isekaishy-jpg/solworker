//! Independent consumer interest and bounded resource-stage propagation.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use super::work_set::WorkSetLease;
use crate::task::{SWCompletion, Subscription};

/// A configured resource rank. Smaller ranks are selected first, independently
/// of the Low/Mid/High execution capacity classes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SWPriority(u16);

impl SWPriority {
    pub const fn new(rank: u16) -> Self {
        Self(rank)
    }
    pub const fn rank(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWDemandError {
    UnknownPriority,
    Gone,
    Full,
    Closed,
}

impl fmt::Display for SWDemandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPriority => f.write_str("resource priority is not configured"),
            Self::Gone => f.write_str("resource stage is no longer pending"),
            Self::Full => f.write_str("demand lease capacity is full"),
            Self::Closed => f.write_str("producer scheduler is closed"),
        }
    }
}
impl std::error::Error for SWDemandError {}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DemandCommand {
    Promote,
    Refresh(SWPriority),
    Defer,
    Detach,
}

/// One consumer's demand lease. Dropping it removes only that consumer's
/// interest, never the producer's cancellation authority. An interest attached
/// after the producer is claimed still retains the consumer relationship but
/// cannot preempt that running invocation.
pub struct SWDemand {
    lease: u64,
    change: Arc<dyn Fn(u64, DemandCommand) -> Result<(), SWDemandError> + Send + Sync>,
    binding: Option<Arc<ConsumerBinding>>,
}

struct ConsumerBinding {
    lease: u64,
    change: Arc<dyn Fn(u64, DemandCommand) -> Result<(), SWDemandError> + Send + Sync>,
    state: Mutex<ConsumerBindingState>,
}

struct ConsumerBindingState {
    active: bool,
    work: Option<WorkSetLease>,
    subscription: Option<Subscription>,
}

impl ConsumerBinding {
    fn end(&self) {
        let (work, subscription) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if !state.active {
                return;
            }
            state.active = false;
            (state.work.take(), state.subscription.take())
        };
        // Neither the consumer-set lock nor this binding lock is held while
        // the scheduler changes demand or the subscription is detached.
        let _ = (self.change)(self.lease, DemandCommand::Detach);
        drop(subscription);
        drop(work);
    }
}

impl SWDemand {
    pub(crate) fn new(
        lease: u64,
        change: Arc<dyn Fn(u64, DemandCommand) -> Result<(), SWDemandError> + Send + Sync>,
    ) -> Self {
        Self {
            lease,
            change,
            binding: None,
        }
    }

    /// Binds this consumer to a separate work set. Set cancellation, producer
    /// completion, and observer drop each release the lease exactly once.
    pub(crate) fn bind_consumer(mut self, work: WorkSetLease, completion: &SWCompletion) -> Self {
        let binding = Arc::new(ConsumerBinding {
            lease: self.lease,
            change: Arc::clone(&self.change),
            state: Mutex::new(ConsumerBindingState {
                active: true,
                work: Some(work.clone()),
                subscription: None,
            }),
        });
        let weak = Arc::downgrade(&binding);
        work.register_cancel(Box::new(move || {
            if let Some(binding) = weak.upgrade() {
                binding.end();
            }
        }));
        let weak = Arc::downgrade(&binding);
        let subscription = completion.subscribe_cancelable(Box::new(move |_| {
            if let Some(binding) = weak.upgrade() {
                binding.end();
            }
        }));
        let unused = {
            let mut state = binding
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.active {
                state.subscription = Some(subscription);
                None
            } else {
                Some(subscription)
            }
        };
        drop(unused);
        self.binding = Some(binding);
        self
    }

    /// Moves still-pending resource work before other work at equal rank.
    pub fn promote(&self) -> Result<(), SWDemandError> {
        (self.change)(self.lease, DemandCommand::Promote)
    }
    /// Changes interest and reactivates this consumer if it was deferred.
    pub fn refresh(&self, priority: SWPriority) -> Result<(), SWDemandError> {
        (self.change)(self.lease, DemandCommand::Refresh(priority))
    }
    /// Keeps background interest but removes this consumer's active demand.
    pub fn defer(&self) -> Result<(), SWDemandError> {
        (self.change)(self.lease, DemandCommand::Defer)
    }
}

impl Drop for SWDemand {
    fn drop(&mut self) {
        if let Some(binding) = &self.binding {
            binding.end();
        } else {
            let _ = (self.change)(self.lease, DemandCommand::Detach);
        }
    }
}

/// Versioned aggregate sent to a provider after the scheduler lock is released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWDemandSnapshot {
    pub priority: SWPriority,
    pub active: bool,
    pub version: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DemandSelection {
    pub(crate) priority: Option<SWPriority>,
    pub(crate) active: bool,
    pub(crate) tie: i128,
    pub(crate) version: u64,
}

pub(crate) type ProviderHook = Arc<dyn Fn(SWDemandSnapshot) + Send + Sync>;

pub(crate) struct DemandChange {
    pub(crate) id: u64,
    pub(crate) selection: DemandSelection,
    pub(crate) provider: Option<(ProviderHook, SWDemandSnapshot)>,
}

struct Lease {
    node: u64,
    priority: SWPriority,
    active: bool,
}
struct Node {
    baseline: Option<SWPriority>,
    parents: Vec<u64>,
    children: Vec<u64>,
    leases: Vec<u64>,
    selection: DemandSelection,
    provider: Option<ProviderHook>,
    dirty: bool,
    published: bool,
    published_tie: i128,
}

/// Control-lock-owned graph. Dirty nodes are enqueued once, so outstanding
/// propagation is bounded by the admitted record count. Service does at most
/// `budget` node visits; no provider hook is called here.
pub(crate) struct DemandState {
    bands: Vec<SWPriority>,
    lease_limit: usize,
    nodes: HashMap<u64, Node>,
    leases: HashMap<u64, Lease>,
    dirty: VecDeque<u64>,
    next_lease: u64,
    front: i128,
    back: i128,
    version: u64,
}

impl DemandState {
    pub(crate) fn new(mut bands: Vec<SWPriority>, lease_limit: usize) -> Self {
        bands.sort_unstable();
        bands.dedup();
        Self {
            bands,
            lease_limit,
            nodes: HashMap::new(),
            leases: HashMap::new(),
            dirty: VecDeque::new(),
            next_lease: 1,
            front: -1,
            back: 1,
            version: 1,
        }
    }

    pub(crate) fn contains(&self, priority: SWPriority) -> bool {
        self.bands.binary_search(&priority).is_ok()
    }

    pub(crate) fn enabled(&self) -> bool {
        !self.bands.is_empty()
    }

    pub(crate) fn register(
        &mut self,
        id: u64,
        baseline: Option<SWPriority>,
        prerequisites: &[u64],
        provider: Option<ProviderHook>,
    ) -> Result<(), SWDemandError> {
        if baseline.is_some_and(|priority| !self.contains(priority)) {
            return Err(SWDemandError::UnknownPriority);
        }
        let parents: Vec<_> = prerequisites
            .iter()
            .copied()
            .filter(|id| self.nodes.contains_key(id))
            .collect();
        let selection = DemandSelection {
            priority: baseline,
            active: baseline.is_some(),
            tie: self.next_back(),
            version: self.next_version(),
        };
        self.nodes.insert(
            id,
            Node {
                baseline,
                parents: parents.clone(),
                children: Vec::new(),
                leases: Vec::new(),
                selection,
                provider,
                dirty: false,
                published: false,
                published_tie: selection.tie,
            },
        );
        for parent in parents {
            if let Some(node) = self.nodes.get_mut(&parent) {
                node.children.push(id);
            }
            self.mark_dirty(parent);
        }
        self.mark_dirty(id);
        Ok(())
    }

    pub(crate) fn remove(&mut self, id: u64) {
        let Some(node) = self.nodes.remove(&id) else {
            return;
        };
        self.dirty.retain(|queued| *queued != id);
        for lease in node.leases {
            self.leases.remove(&lease);
        }
        for parent in node.parents {
            if let Some(node) = self.nodes.get_mut(&parent) {
                node.children.retain(|child| *child != id);
            }
            self.mark_dirty(parent);
        }
        for child in node.children {
            if let Some(node) = self.nodes.get_mut(&child) {
                node.parents.retain(|parent| *parent != id);
            }
        }
    }

    pub(crate) fn attach(&mut self, id: u64, priority: SWPriority) -> Result<u64, SWDemandError> {
        if !self.contains(priority) {
            return Err(SWDemandError::UnknownPriority);
        }
        if !self.nodes.contains_key(&id) {
            return Err(SWDemandError::Gone);
        }
        if self.leases.len() >= self.lease_limit {
            return Err(SWDemandError::Full);
        }
        let node = self
            .nodes
            .get_mut(&id)
            .expect("checked pending demand node");
        let lease = self.next_lease;
        self.next_lease = lease
            .checked_add(1)
            .expect("demand lease identity exhausted");
        node.leases.push(lease);
        self.leases.insert(
            lease,
            Lease {
                node: id,
                priority,
                active: true,
            },
        );
        self.mark_dirty(id);
        Ok(lease)
    }

    pub(crate) fn change(
        &mut self,
        lease: u64,
        command: DemandCommand,
    ) -> Result<(), SWDemandError> {
        if let DemandCommand::Refresh(priority) = command
            && !self.contains(priority)
        {
            return Err(SWDemandError::UnknownPriority);
        }
        let Some(interest) = self.leases.get_mut(&lease) else {
            return Err(SWDemandError::Gone);
        };
        let id = interest.node;
        match command {
            DemandCommand::Promote => {
                if let Some(node) = self.nodes.get_mut(&id) {
                    node.selection.tie = self.front;
                    self.front = self
                        .front
                        .checked_sub(1)
                        .expect("demand tie identity exhausted");
                }
            }
            DemandCommand::Refresh(priority) => {
                if interest.priority != priority || !interest.active {
                    interest.priority = priority;
                    interest.active = true;
                    if let Some(node) = self.nodes.get_mut(&id) {
                        node.selection.tie = self.back;
                        self.back = self
                            .back
                            .checked_add(1)
                            .expect("demand tie identity exhausted");
                    }
                }
            }
            DemandCommand::Defer => interest.active = false,
            DemandCommand::Detach => {
                self.leases.remove(&lease);
                if let Some(node) = self.nodes.get_mut(&id) {
                    node.leases.retain(|attached| *attached != lease);
                }
            }
        }
        self.mark_dirty(id);
        Ok(())
    }

    pub(crate) fn selection(&self, id: u64) -> Option<DemandSelection> {
        self.nodes.get(&id).map(|node| node.selection)
    }
    pub(crate) fn pending_updates(&self) -> bool {
        !self.dirty.is_empty()
    }

    pub(crate) fn service(&mut self, budget: usize) -> Vec<DemandChange> {
        let mut changed = Vec::new();
        for _ in 0..budget {
            let Some(id) = self.dirty.pop_front() else {
                break;
            };
            let Some(node) = self.nodes.get(&id) else {
                continue;
            };
            let (priority, active) = self.aggregate(node);
            let previous = node.selection;
            let published = node.published;
            let published_tie = node.published_tie;
            let parents = node.parents.clone();
            let provider = node.provider.clone();
            if let Some(node) = self.nodes.get_mut(&id) {
                node.dirty = false;
            }
            if published
                && previous.priority == priority
                && previous.active == active
                && previous.tie == published_tie
            {
                continue;
            }
            let version = self.next_version();
            let selection = DemandSelection {
                priority,
                active,
                tie: previous.tie,
                version,
            };
            if let Some(node) = self.nodes.get_mut(&id) {
                node.selection = selection;
                node.published = true;
                node.published_tie = selection.tie;
            }
            let provider = provider.and_then(|hook| {
                priority.map(|priority| {
                    (
                        hook,
                        SWDemandSnapshot {
                            priority,
                            active,
                            version,
                        },
                    )
                })
            });
            changed.push(DemandChange {
                id,
                selection,
                provider,
            });
            for parent in parents {
                self.mark_dirty(parent);
            }
        }
        changed
    }

    fn aggregate(&self, node: &Node) -> (Option<SWPriority>, bool) {
        let mut active: Option<SWPriority> = None;
        let mut deferred: Option<SWPriority> = None;
        for lease in &node.leases {
            if let Some(interest) = self.leases.get(lease) {
                let target = if interest.active {
                    &mut active
                } else {
                    &mut deferred
                };
                *target =
                    Some(target.map_or(interest.priority, |rank| rank.min(interest.priority)));
            }
        }
        for child in &node.children {
            if let Some(child) = self.nodes.get(child)
                && let Some(rank) = child.selection.priority
            {
                let target = if child.selection.active {
                    &mut active
                } else {
                    &mut deferred
                };
                *target = Some(target.map_or(rank, |current| current.min(rank)));
            }
        }
        match active {
            Some(rank) => (
                Some(node.baseline.map_or(rank, |base| base.min(rank))),
                true,
            ),
            None => match deferred {
                Some(rank) => (Some(rank), false),
                None => (node.baseline, node.baseline.is_some()),
            },
        }
    }

    fn mark_dirty(&mut self, id: u64) {
        if let Some(node) = self.nodes.get_mut(&id)
            && !node.dirty
        {
            node.dirty = true;
            self.dirty.push_back(id);
        }
    }

    fn next_version(&mut self) -> u64 {
        let version = self.version;
        self.version = version.checked_add(1).expect("demand version exhausted");
        version
    }

    fn next_back(&mut self) -> i128 {
        let tie = self.back;
        self.back = tie.checked_add(1).expect("demand tie identity exhausted");
        tie
    }
}
