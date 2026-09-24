//! Finite arrivals and a frame-work spike, followed by measured recovery.
use super::common::world::{Executor, Schedule, World};
use solworker::{
    SWExecutionClass, SWOutcome, SWOwnedLimits, SWOwnerError, SWPhase, SWPumpBudget, SWSpawnError,
    SWSpawnOptions, SWTask,
};
use std::collections::VecDeque;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub enum Pressure {
    Runnable,
    Records,
}

impl Pressure {
    pub const ALL: [Self; 2] = [Self::Runnable, Self::Records];
    fn limits(self) -> SWOwnedLimits {
        match self {
            Self::Runnable => SWOwnedLimits::new(64, 0, [2, 2, 32], [1; 3]).unwrap(),
            Self::Records => SWOwnedLimits::new(8, 0, [64; 3], [1; 3]).unwrap(),
        }
    }
    pub fn record_limit(self) -> usize {
        match self {
            Self::Runnable => 64,
            Self::Records => 8,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Config {
    pub baseline_frames: usize,
    pub burst_frames: usize,
    pub burst_arrivals: usize,
    pub settled_frames: usize,
    pub frame_rounds: usize,
    pub spike_multiplier: usize,
    pub job_rounds: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            baseline_frames: 24,
            burst_frames: 3,
            burst_arrivals: 64,
            settled_frames: 64,
            frame_rounds: 16,
            spike_multiplier: 8,
            job_rounds: 65536,
        }
    }
}

pub struct Sample {
    pub phase: &'static str,
    pub frame_us: f64,
    pub pending_cpu: usize,
    pub unobserved_tasks: usize,
    pub pending_publication: usize,
    pub outstanding: usize,
    pub scheduler_records: usize,
    pub admission_retries: usize,
    pub frame_retries: usize,
    pub delivery_retries: usize,
}

pub struct Report {
    pub samples: Vec<Sample>,
    pub generated: usize,
    pub published: usize,
    pub checksum: u64,
    pub peak_backlog: usize,
    pub peak_records: usize,
    pub cpu_recovery_frames: usize,
    pub cpu_recovery_us: f64,
    pub recovery_frames: usize,
    pub recovery_us: f64,
}

struct Published {
    owner: ThreadId,
    counts: Vec<usize>,
    total: usize,
    checksum: u64,
}

type Work = Box<dyn FnOnce() -> u64 + Send>;
type Publication = Box<dyn FnOnce(&mut Published)>;

fn kernel(mut value: u64, rounds: usize) -> u64 {
    for _ in 0..rounds {
        value = black_box(value.wrapping_mul(37).rotate_left(13) ^ 17);
    }
    value
}

// Only correctness runs gate the first burst admission, making Full
// deterministic rather than depending on CPU speed. Drop also releases on panic.
struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Drop for Gate {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

pub fn run(schedule: Schedule, pressure: Pressure, config: Config, verify_frames: bool) -> Report {
    assert!(config.baseline_frames > 0 && config.burst_frames > 0 && config.settled_frames > 0);
    let expected_jobs = config.baseline_frames + config.burst_frames * config.burst_arrivals;
    let calls = Arc::new(
        (0..expected_jobs)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let mut executor = Executor::with_limits(1, pressure.limits());
    let mut world = World::new(256, 32);
    let mut oracle = verify_frames.then(|| World::new(256, 32));
    let mut owner = executor
        .runtime
        .owner(
            Published {
                owner: thread::current().id(),
                counts: vec![0; expected_jobs],
                total: 0,
                checksum: 0,
            },
            NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();
    let phase = SWPhase(1);
    owner.set_phase(phase).unwrap();
    let mut pending: VecDeque<(usize, Work)> = VecDeque::new();
    let mut tasks: Vec<(usize, SWTask<u64>)> = Vec::new();
    let mut publication: VecDeque<Publication> = VecDeque::new();
    let mut generated = 0;
    let mut frame = 0;
    let mut recovery_start = None;
    let mut drained = None;
    let mut cpu_drained = None;
    let mut delivery_pressure_seen = false;
    let mut quiet = 0;
    let mut samples = Vec::new();
    let mut peak_backlog = 0;
    let mut peak_records = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    while quiet < config.settled_frames {
        assert!(Instant::now() < deadline, "burst failed to drain");
        let burst_end = config.baseline_frames + config.burst_frames;
        let label = if frame < config.baseline_frames {
            "baseline"
        } else if frame < burst_end {
            "burst"
        } else if drained.is_none() {
            "recovery"
        } else {
            "settled"
        };
        if frame == burst_end {
            recovery_start = Some(Instant::now());
        }
        let started = Instant::now();
        let arrivals = match label {
            "baseline" => 1,
            "burst" => config.burst_arrivals,
            _ => 0,
        };
        let gate = (verify_frames && frame == config.baseline_frames)
            .then(|| Gate(Arc::new((Mutex::new(false), Condvar::new()))));
        for _ in 0..arrivals {
            let id = generated;
            generated += 1;
            let calls = Arc::clone(&calls);
            let gate = gate.as_ref().map(|gate| Arc::clone(&gate.0));
            pending.push_back((
                id,
                Box::new(move || {
                    if let Some(gate) = gate {
                        drop(
                            gate.1
                                .wait_while(gate.0.lock().unwrap(), |released| !*released)
                                .unwrap(),
                        );
                    }
                    calls[id].fetch_add(1, Ordering::Relaxed);
                    kernel(id as u64, config.job_rounds)
                }),
            ));
        }
        peak_backlog = peak_backlog.max(generated - owner.state().total);
        let mut admission_retries = 0;
        while let Some((id, operation)) = pending.pop_front() {
            let class = if id % 2 == 0 {
                SWExecutionClass::Low
            } else {
                SWExecutionClass::Mid
            };
            match executor
                .runtime
                .lane(class)
                .try_spawn(SWSpawnOptions::default(), operation)
            {
                Ok((task, _)) => tasks.push((id, task)),
                Err(rejected) => {
                    assert_eq!(rejected.reason, SWSpawnError::Full);
                    // Preserve the exact uninvoked operation. A later frame
                    // services this FIFO backlog; never drop work on pressure.
                    pending.push_front((id, rejected.operation));
                    admission_retries += 1;
                    break;
                }
            }
        }
        drop(gate);
        let before = executor.runtime.progress().scheduler;
        let records_before = before.waiting
            + before.ready
            + before.deferred
            + before.handed
            + before.running
            + before.finalizing;
        peak_records = peak_records.max(records_before);
        assert!(records_before <= pressure.record_limit());
        let rounds = config.frame_rounds
            * if label == "burst" {
                config.spike_multiplier
            } else {
                1
            };
        let draws = world.frame(&mut executor, schedule, rounds);
        // Keep optional serial validation out of the benchmark's timed interval.
        let comparison = oracle.as_ref().map(|_| draws.to_vec());
        let mut index = 0;
        while index < tasks.len() {
            if let Some(outcome) = tasks[index].1.try_take() {
                let (id, _) = tasks.swap_remove(index);
                let SWOutcome::Success(value) = outcome else {
                    panic!("burst job failed");
                };
                publication.push_back(Box::new(move |state| {
                    assert_eq!(thread::current().id(), state.owner);
                    state.counts[id] += 1;
                    state.total += 1;
                    state.checksum = state.checksum.wrapping_add(value);
                }));
            } else {
                index += 1;
            }
        }
        let mut delivery_retries = 0;
        while let Some(callback) = publication.pop_front() {
            if let Err(rejected) = owner.try_post(phase, callback) {
                assert_eq!(rejected.reason, SWOwnerError::Full);
                publication.push_front(rejected.callback);
                delivery_retries += 1;
                break;
            }
        }
        // Deliberately small host service budget makes publication pressure
        // measurable independently of CPU completion.
        delivery_pressure_seen |= delivery_retries != 0;
        // Correctness runs hold publication until Full has been observed;
        // measurements use the fixed one-callback-per-frame budget throughout.
        if !verify_frames || delivery_pressure_seen {
            owner.pump(phase, SWPumpBudget::new(1)).unwrap();
        }
        let elapsed = started.elapsed().as_secs_f64() * 1e6;
        if let Some(oracle) = oracle.as_mut() {
            assert_eq!(
                comparison.unwrap(),
                oracle.frame(&mut executor, Schedule::Serial, rounds)
            );
        }
        let progress = executor.runtime.progress();
        let cpu = progress.scheduler;
        let records =
            cpu.waiting + cpu.ready + cpu.deferred + cpu.handed + cpu.running + cpu.finalizing;
        assert!(records <= pressure.record_limit());
        peak_records = peak_records.max(records);
        let outstanding = generated - owner.state().total;
        samples.push(Sample {
            phase: label,
            frame_us: elapsed,
            pending_cpu: pending.len(),
            unobserved_tasks: tasks.len(),
            pending_publication: publication.len()
                + progress.owner.ready
                + progress.owner.waiting
                + progress.owner.cleanup,
            outstanding,
            scheduler_records: records_before.max(records),
            admission_retries,
            frame_retries: world.admission_retries,
            delivery_retries,
        });
        if frame >= burst_end
            && cpu_drained.is_none()
            && pending.is_empty()
            && tasks.is_empty()
            && records == 0
            && cpu.handoff_wrappers == [0; 3]
            && progress.active_leases == 0
        {
            cpu_drained = Some((
                frame - burst_end + 1,
                recovery_start.unwrap().elapsed().as_secs_f64() * 1e6,
            ));
        }
        if frame >= burst_end
            && drained.is_none()
            && outstanding == 0
            && pending.is_empty()
            && tasks.is_empty()
            && publication.is_empty()
            && records == 0
            && cpu.handoff_wrappers == [0; 3]
            && progress.active_leases == 0
        {
            drained = Some((
                frame - burst_end + 1,
                recovery_start.unwrap().elapsed().as_secs_f64() * 1e6,
            ));
        }
        if label == "settled" {
            quiet += 1;
        }
        frame += 1;
    }
    assert_eq!(generated, expected_jobs);
    assert_eq!(owner.state().total, generated);
    assert!(owner.state().counts.iter().all(|count| *count == 1));
    assert!(calls.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    let expected = (0..expected_jobs)
        .map(|id| kernel(id as u64, config.job_rounds))
        .fold(0_u64, u64::wrapping_add);
    assert_eq!(owner.state().checksum, expected);
    let checksum = owner.state().checksum;
    owner.close();
    executor.runtime.shutdown().unwrap();
    let (recovery_frames, recovery_us) = drained.unwrap();
    let (cpu_recovery_frames, cpu_recovery_us) = cpu_drained.unwrap();
    Report {
        samples,
        generated,
        published: generated,
        checksum,
        peak_backlog,
        peak_records,
        cpu_recovery_frames,
        cpu_recovery_us,
        recovery_frames,
        recovery_us,
    }
}
