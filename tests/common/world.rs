//! Deterministic synthetic frame work shared by contracts and benchmarks.
//! The algorithms and sizes are fixtures, not native implementations or captures.
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use solworker::{
    SWBatch, SWCallerEligibility, SWExecutionClass, SWLane, SWOutcome, SWOwnedLimits, SWRuntime,
    SWRuntimeConfig, SWSpawnError, SWSpawnOptions, SWTask, SWTaskStatus, SWWorkerConfig,
};

#[derive(Clone, Copy, Debug)]
pub enum Schedule {
    Serial,
    CallerSplit,
    ScopedChunks,
    RetainedWaves,
}

impl Schedule {
    pub const ALL: [Self; 4] = [
        Self::Serial,
        Self::CallerSplit,
        Self::ScopedChunks,
        Self::RetainedWaves,
    ];
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Draw {
    pub pass: u32,
    pub depth: u32,
    pub instance: usize,
    pub value: u64,
}

struct Instance {
    id: usize,
    // Immutable model data is shared across distinct mutable placements.
    model: Arc<[u64; 32]>,
    pose: [u64; 32],
    effects: [u64; 16],
    pose_frame: u64,
    effect_frame: u64,
}

pub struct World {
    pub admission_retries: usize,
    partitions: Vec<Vec<Instance>>,
    split: usize,
    frame: u64,
    owner: ThreadId,
    draws: Vec<Draw>,
}

pub struct Executor {
    pub runtime: SWRuntime,
    high: SWLane,
    batch: SWBatch,
}

impl Executor {
    pub fn new(workers: usize) -> Self {
        Self::with_limits(
            workers,
            SWOwnedLimits::new(4096, 4096, [4096; 3], [workers; 3]).unwrap(),
        )
    }

    pub fn with_limits(workers: usize, limits: SWOwnedLimits) -> Self {
        let runtime = SWRuntime::builder(
            SWRuntimeConfig::new(workers * 3, [SWWorkerConfig::new(workers); 3]).unwrap(),
        )
        .with_owned_limits(limits)
        .build()
        .unwrap();
        let high = runtime.lane(SWExecutionClass::High);
        let batch = high.batch();
        Self {
            runtime,
            high,
            batch,
        }
    }
}

impl World {
    pub fn new(instances: usize, chunk: usize) -> Self {
        assert!(instances > 0 && chunk > 0);
        let models: Vec<_> = (0..8)
            .map(|model| Arc::new(std::array::from_fn(|bone| (model * 37 + bone + 1) as u64)))
            .collect();
        let mut partitions = Vec::new();
        let mut split = 0;
        for parity in 0..2 {
            let mut roots = Vec::new();
            for id in (parity..instances).step_by(2) {
                roots.push(Instance {
                    id,
                    model: Arc::clone(&models[id % models.len()]),
                    pose: [0; 32],
                    effects: [0; 16],
                    pose_frame: 0,
                    effect_frame: 0,
                });
                if roots.len() == chunk {
                    partitions.push(std::mem::take(&mut roots));
                }
            }
            if !roots.is_empty() {
                partitions.push(roots);
            }
            if parity == 0 {
                split = partitions.len();
            }
        }
        assert!(partitions.len() <= 4096);
        Self {
            admission_retries: 0,
            partitions,
            split,
            frame: 0,
            owner: thread::current().id(),
            draws: Vec::new(),
        }
    }

    pub fn frame(&mut self, executor: &mut Executor, schedule: Schedule, rounds: usize) -> &[Draw] {
        assert_eq!(thread::current().id(), self.owner);
        self.admission_retries = 0;
        self.frame += 1;
        let frame = self.frame;
        // Mutable pose must settle before effects read it. Each stage owns
        // disjoint instance state; shared model data remains immutable.
        for stage in [Stage::Pose, Stage::Effects] {
            let process = |partitions: &mut [Vec<Instance>]| {
                for partition in partitions {
                    run_stage(partition, stage, frame, rounds);
                }
            };
            match schedule {
                Schedule::Serial => process(&mut self.partitions),
                Schedule::CallerSplit if matches!(stage, Stage::Effects) => {
                    // The reference split joins before owner-side effects.
                    process(&mut self.partitions);
                }
                Schedule::CallerSplit => {
                    let (even, odd) = self.partitions.split_at_mut(self.split);
                    let (worker, owner) = executor
                        .high
                        .join_with_owner(|| process(odd), || process(even))
                        .unwrap();
                    worker.unwrap();
                    owner.unwrap();
                }
                Schedule::ScopedChunks => {
                    executor
                        .high
                        .for_each_chunk(
                            &mut self.partitions,
                            NonZeroUsize::new(1).unwrap(),
                            |_, chunks| process(chunks),
                        )
                        .unwrap()
                        .unwrap();
                }
                Schedule::RetainedWaves => {
                    let group = executor.batch.begin().unwrap();
                    let mut tasks = Vec::with_capacity(self.partitions.len());
                    let deadline = Instant::now() + Duration::from_secs(10);
                    for partition in &mut self.partitions {
                        let mut input = std::mem::take(partition);
                        let mut operation = move || {
                            run_stage(&mut input, stage, frame, rounds);
                            input
                        };
                        let task = loop {
                            match executor.high.submit_or_run_in(
                                group,
                                SWSpawnOptions {
                                    eligibility: SWCallerEligibility::CallerEligible,
                                },
                                operation,
                            ) {
                                Ok((task, _)) => break task,
                                Err(rejected) => {
                                    assert_eq!(rejected.reason, SWSpawnError::Full);
                                    operation = rejected.operation;
                                    self.admission_retries += 1;
                                    assert!(Instant::now() < deadline, "frame admission stalled");
                                    if !group.help_ready().unwrap() {
                                        thread::yield_now();
                                    }
                                }
                            }
                        };
                        tasks.push(task);
                    }
                    group.seal();
                    group.wait_helping().unwrap();
                    for (partition, mut task) in self.partitions.iter_mut().zip(tasks) {
                        let Some(SWOutcome::Success(output)) = task.try_take() else {
                            panic!("frame stage failed");
                        };
                        *partition = output;
                    }
                }
            }
        }
        // Movement changes visibility; combat drives effect packets. Collection
        // and a scene-wide merge remain owner-local after dependent stages.
        self.draws.clear();
        let camera = (frame as usize * 7) % 257;
        for partition in &self.partitions {
            for instance in partition {
                assert_eq!(instance.effect_frame, frame);
                let distance = (instance.id * 11 % 257).abs_diff(camera) as u32;
                if distance > 128 {
                    continue;
                }
                self.draws.push(Draw {
                    pass: 0,
                    depth: distance,
                    instance: instance.id,
                    value: instance.pose.iter().fold(0, |sum, value| sum ^ value),
                });
                if frame % 8 < 4 {
                    self.draws.push(Draw {
                        pass: 1,
                        depth: u32::MAX - distance,
                        instance: instance.id,
                        value: instance.effects.iter().fold(0, |sum, value| sum ^ value),
                    });
                }
            }
        }
        // Fixture order, not a claim about a native comparator or equal-key stability.
        self.draws
            .sort_unstable_by_key(|draw| (draw.pass, draw.depth, draw.instance));
        black_box(&self.draws)
    }
}

#[derive(Clone, Copy)]
enum Stage {
    Pose,
    Effects,
}

fn mix(mut value: u64, rounds: usize) -> u64 {
    for _ in 0..rounds {
        value = black_box(
            value.wrapping_mul(6364136223846793005).rotate_left(17) ^ 1442695040888963407,
        );
    }
    value
}

fn run_stage(instances: &mut [Instance], stage: Stage, frame: u64, rounds: usize) {
    for instance in instances {
        match stage {
            Stage::Pose => {
                for (bone, output) in instance.pose.iter_mut().enumerate() {
                    *output = mix(instance.model[bone] ^ frame ^ instance.id as u64, rounds);
                }
                instance.pose_frame = frame;
            }
            Stage::Effects => {
                assert_eq!(instance.pose_frame, frame);
                for (emitter, output) in instance.effects.iter_mut().enumerate() {
                    *output = mix(instance.pose[emitter * 2] ^ *output, rounds / 2);
                }
                instance.effect_frame = frame;
            }
        }
    }
}

/// Bounded by the host's frame interval, not a sleep pretending to be CPU work.
/// Drop signals stop even when a correctness assertion unwinds.
pub struct Background {
    stop: Arc<AtomicBool>,
    tasks: Vec<SWTask<u64>>,
}

impl Background {
    pub fn start(runtime: &SWRuntime) -> Self {
        let mut background = Self {
            stop: Arc::new(AtomicBool::new(false)),
            tasks: Vec::new(),
        };
        let (started, received) = mpsc::channel();
        for class in [SWExecutionClass::Low, SWExecutionClass::Mid] {
            let stop = Arc::clone(&background.stop);
            let started = started.clone();
            let (task, _) = runtime
                .lane(class)
                .try_spawn(SWSpawnOptions::default(), move || {
                    started.send(()).unwrap();
                    let mut blocks = 0;
                    let mut value = 1;
                    while !stop.load(Ordering::Relaxed) {
                        value = mix(value, 4096);
                        blocks += 1;
                    }
                    black_box(value);
                    blocks
                })
                .unwrap();
            background.tasks.push(task);
        }
        for _ in 0..2 {
            received.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        background
    }

    pub fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.tasks
            .drain(..)
            .map(|mut task| {
                assert_eq!(
                    task.completion()
                        .wait_timeout(Duration::from_secs(10))
                        .unwrap(),
                    Some(SWTaskStatus::Succeeded)
                );
                let Some(SWOutcome::Success(blocks)) = task.try_take() else {
                    panic!("background failed");
                };
                blocks
            })
            .sum()
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}
