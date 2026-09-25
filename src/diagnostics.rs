//! Opt-in bounded diagnostic timeline. Disabled in default builds.
//!
//! Start once before creating runtimes. Events perturb scheduling and may be
//! dropped at capacity; never use them as completion or lifetime proofs.
use std::cell::Cell;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::ThreadId;
use std::time::Instant;

const CAPACITY: usize = 65_536;
const THREADS: usize = 64;

/// A process-local observation. Runtime and record IDs are diagnostic
/// identifiers, not public handles. Timestamps share the start origin.
#[derive(Clone, Copy, Debug)]
pub struct SWTraceEvent {
    pub elapsed_ns: u64,
    pub thread: ThreadId,
    /// Native Windows TID for joining OS scheduling observations, when available.
    pub native_thread_id: Option<u32>,
    /// Runtime identity distinguishes record IDs from separate runtimes.
    pub runtime: u64,
    pub event: &'static str,
    pub id: u64,
    pub related: u64,
}

/// Identity of the owned job executing or settling on this thread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWTraceJob {
    pub runtime: u64,
    pub id: u64,
    pub group: Option<u64>,
}

struct Buffer {
    events: Vec<SWTraceEvent>,
    dropped: u64,
}
struct Session {
    origin: Instant,
    buffers: Mutex<Vec<Arc<Mutex<Buffer>>>>,
}
static SESSION: OnceLock<Session> = OnceLock::new();
thread_local! {
    static LOCAL: RefCell<Option<Arc<Mutex<Buffer>>>> = const { RefCell::new(None) };
    static CURRENT_JOB: Cell<Option<SWTraceJob>> = const { Cell::new(None) };
}

/// Restores a suspended job when helping reenters the scheduler, including
/// during unwinding. The guard cannot move to another thread.
pub(crate) struct JobGuard {
    previous: Option<SWTraceJob>,
    _not_send: PhantomData<Rc<()>>,
}

impl JobGuard {
    pub(crate) fn enter(job: SWTraceJob) -> Self {
        let previous = CURRENT_JOB.with(|slot| slot.replace(Some(job)));
        Self {
            previous,
            _not_send: PhantomData,
        }
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        CURRENT_JOB.with(|slot| slot.set(self.previous));
    }
}

/// One process-wide trace with fixed per-thread capacity. Drain outside measured
/// frame work. The caller owns budgeting of drained storage and output.
pub struct SWTrace;
impl SWTrace {
    /// Returns the owned job currently executing or settling on this thread.
    /// Nested helping temporarily replaces the identity and restores it on return.
    pub fn current_job() -> Option<SWTraceJob> {
        CURRENT_JOB.with(Cell::get)
    }
    /// Starts the single session. Returns false if already started. There is no
    /// reset while publishers may exist. Without a start, probes collect nothing.
    pub fn start(origin: Instant) -> bool {
        SESSION
            .set(Session {
                origin,
                buffers: Mutex::new(Vec::new()),
            })
            .is_ok()
    }

    /// Appends observations and returns the cumulative dropped-event count.
    /// This is a diagnostic snapshot, not a global ordering or settlement fence.
    pub fn drain(output: &mut Vec<SWTraceEvent>) -> u64 {
        let Some(session) = SESSION.get() else {
            return 0;
        };
        let buffers = session.buffers.lock().unwrap_or_else(|e| e.into_inner());
        let mut dropped = 0;
        for buffer in buffers.iter() {
            let mut buffer = buffer.lock().unwrap_or_else(|e| e.into_inner());
            output.append(&mut buffer.events);
            dropped += buffer.dropped;
        }
        dropped
    }
}

pub(crate) fn record(event: &'static str, runtime: u64, id: u64, related: u64) {
    if SESSION.get().is_none() {
        return;
    }
    record_at(event, runtime, id, related, Instant::now());
}

pub(crate) fn record_at(event: &'static str, runtime: u64, id: u64, related: u64, at: Instant) {
    let Some(session) = SESSION.get() else { return };
    let observation = SWTraceEvent {
        elapsed_ns: at
            .saturating_duration_since(session.origin)
            .as_nanos()
            .min(u64::MAX as u128) as u64,
        thread: std::thread::current().id(),
        native_thread_id: native_thread_id(),
        runtime,
        event,
        id,
        related,
    };
    let _ = LOCAL.try_with(|local| {
        let mut local = local.borrow_mut();
        if local.is_none() {
            let mut buffers = session.buffers.lock().unwrap_or_else(|e| e.into_inner());
            // The fixture uses far fewer threads. Refuse extra publishers rather
            // than grow without a bound; saturation is visible as a sentinel.
            if buffers.len() >= THREADS {
                if let Some(first) = buffers.first() {
                    first.lock().unwrap_or_else(|e| e.into_inner()).dropped += 1;
                }
                return;
            }
            let buffer = Arc::new(Mutex::new(Buffer {
                events: Vec::with_capacity(CAPACITY),
                dropped: 0,
            }));
            buffers.push(Arc::clone(&buffer));
            *local = Some(buffer);
        }
        let mut buffer = local
            .as_ref()
            .expect("registered trace buffer")
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if buffer.events.len() < CAPACITY {
            buffer.events.push(observation);
        } else {
            buffer.dropped += 1;
        }
    });
}

pub(crate) fn clock() -> Option<Instant> {
    SESSION.get().map(|_| Instant::now())
}

#[cfg(windows)]
fn native_thread_id() -> Option<u32> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentThreadId() -> u32;
    }
    // SAFETY: No arguments, handles or pointer lifetimes are involved.
    Some(unsafe { GetCurrentThreadId() })
}

#[cfg(not(windows))]
fn native_thread_id() -> Option<u32> {
    None
}

/// Diagnostic execution cycles, never converted to a portable time estimate.
#[cfg(windows)]
pub(crate) fn cycles() -> Option<u64> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentThread() -> *mut std::ffi::c_void;
        fn QueryThreadCycleTime(thread: *mut std::ffi::c_void, cycles: *mut u64) -> i32;
    }
    let mut value = 0;
    // SAFETY: Current-thread pseudo handle needs no closing; output is writable.
    (unsafe { QueryThreadCycleTime(GetCurrentThread(), &mut value) } != 0).then_some(value)
}

#[cfg(not(windows))]
pub(crate) fn cycles() -> Option<u64> {
    None
}
