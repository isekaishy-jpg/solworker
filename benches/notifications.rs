#[path = "../tests/common/world.rs"]
#[allow(dead_code)]
mod world;

use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use solworker::{
    SWDependencyPolicy, SWExecutionClass, SWNotifyLimits, SWOwnedLimits, SWPhase, SWPumpBudget,
    SWRuntime, SWRuntimeConfig, SWSpawnOptions, SWWorkerConfig,
};
use world::{Executor, Schedule, World};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Disabled,
    EnabledIdle,
    NarrowInert,
    NarrowNative,
    BroadInert,
    BroadNative,
}

impl Mode {
    const ALL: [Self; 6] = [
        Self::Disabled,
        Self::EnabledIdle,
        Self::NarrowInert,
        Self::NarrowNative,
        Self::BroadInert,
        Self::BroadNative,
    ];

    fn enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    fn narrow(self) -> bool {
        matches!(self, Self::NarrowInert | Self::NarrowNative)
    }

    fn broad(self) -> bool {
        matches!(self, Self::BroadInert | Self::BroadNative)
    }

    fn native(self) -> bool {
        matches!(self, Self::NarrowNative | Self::BroadNative)
    }
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value.parse().expect("invalid benchmark setting")
    })
}

fn percentile(sorted: &[f64], percent: usize) -> f64 {
    sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)]
}

fn main() {
    let frames = setting("SW_NOTIFY_BENCH_FRAMES", 250);
    let warmup = setting("SW_NOTIFY_BENCH_WARMUP", 25);
    let instances = setting("SW_NOTIFY_BENCH_INSTANCES", 256);
    let chunk = setting("SW_NOTIFY_BENCH_CHUNK", 32);
    let workers = setting("SW_NOTIFY_BENCH_WORKERS", 1);
    let rounds = setting("SW_NOTIFY_BENCH_ROUNDS", 16);
    assert!(frames > 0 && workers > 0);

    let mut raw = std::env::var_os("SW_RUN_OUTPUT_DIR").map(|directory| {
        let path = std::path::PathBuf::from(directory).join("notification-samples.csv");
        let mut file = BufWriter::new(std::fs::File::create(path).unwrap());
        writeln!(file, "mode,frame,elapsed_us").unwrap();
        file
    });
    println!(
        "mode,frames,warmup,stages,setup_us,bindings,p50_us,p95_us,p99_us,max_us,mean_us,signals,changed_frames,checksum"
    );
    let selected = std::env::var("SW_NOTIFY_BENCH_MODE").ok();
    if let Some(name) = &selected {
        assert!(
            Mode::ALL.iter().any(|mode| format!("{mode:?}") == *name),
            "unknown SW_NOTIFY_BENCH_MODE"
        );
    }
    let mut reference_checksum = None;
    for mode in Mode::ALL {
        if selected
            .as_ref()
            .is_some_and(|name| *name != format!("{mode:?}"))
        {
            continue;
        }
        let setup_start = Instant::now();
        let limits = SWOwnedLimits::new(4096, 4096, [4096; 3], [workers; 3]).unwrap();
        let notifications = mode.enabled().then_some(SWNotifyLimits {
            routes: 1,
            bindings: 2,
        });
        let mut executor = Executor::with_notification_limits(workers, limits, notifications);
        let mut world = World::new(instances, chunk);
        let signals = Arc::new(AtomicUsize::new(0));
        let signal_count = Arc::clone(&signals);
        let host_thread = std::thread::current();
        // Thread::unpark is a latched host primitive. The retained-wave fixture
        // still uses its normal helping join; this measures adapter publication.
        let mut route = mode.enabled().then(|| {
            executor
                .runtime
                .notification_route(move || {
                    signal_count.fetch_add(1, Ordering::Relaxed);
                    if mode.native() {
                        host_thread.unpark();
                    }
                    Ok(())
                })
                .unwrap()
        });
        let _progress_binding = if mode.broad() {
            Some(route.as_mut().unwrap().watch_progress().unwrap())
        } else {
            None
        };
        let setup_us = setup_start.elapsed().as_secs_f64() * 1e6;

        let mut binding_count = 0usize;
        let mut changed_frames = 0usize;
        let mut samples = Vec::with_capacity(frames);
        let mut checksum = 0u64;
        for frame in 0..(warmup + frames) {
            let mut last_stamp = None;
            let started = Instant::now();
            let draws = world.frame_with_group_binding(
                &mut executor,
                Schedule::RetainedWaves,
                rounds,
                |group| {
                    let route = route.as_mut()?;
                    let binding = if mode.narrow() {
                        binding_count += 1;
                        Some(route.watch_completion(&group.completion()).unwrap())
                    } else {
                        None
                    };
                    if mode.narrow() || mode.broad() {
                        if mode.native() {
                            // Drain the latched permit before arming and rechecking.
                            std::thread::park_timeout(Duration::ZERO);
                        }
                        last_stamp = Some(route.prepare_wait().unwrap());
                    }
                    binding
                },
            );
            let elapsed = started.elapsed().as_secs_f64() * 1e6;
            if let (Some(route), Some(stamp)) = (route.as_ref(), last_stamp)
                && route.changed_since(stamp).unwrap()
            {
                changed_frames += 1;
            }
            if frame >= warmup {
                samples.push(elapsed);
                for draw in draws {
                    checksum = checksum.wrapping_add(black_box(draw.value));
                }
            }
        }
        if let Some(expected) = reference_checksum {
            assert_eq!(checksum, expected);
        }
        reference_checksum = Some(checksum);
        if let Some(file) = raw.as_mut() {
            for (frame, elapsed) in samples.iter().enumerate() {
                writeln!(file, "{mode:?},{frame},{elapsed:.3}").unwrap();
            }
        }
        samples.sort_by(f64::total_cmp);
        println!(
            "{mode:?},{frames},{warmup},{},{setup_us:.3},{binding_count},{:.3},{:.3},{:.3},{:.3},{:.3},{},{changed_frames},{checksum}",
            (warmup + frames) * 2,
            percentile(&samples, 50),
            percentile(&samples, 95),
            percentile(&samples, 99),
            samples[samples.len() - 1],
            samples.iter().sum::<f64>() / samples.len() as f64,
            signals.load(Ordering::Relaxed),
        );
        if let Some(route) = route.as_mut() {
            route.close().unwrap();
        }
        executor.runtime.shutdown().unwrap();
    }
    if let Some(mut file) = raw {
        file.flush().unwrap();
    }
    if selected.is_none() {
        protocol_cases();
        tail_cases();
    }
}

fn tail_row(name: &str, mut samples: Vec<f64>) {
    samples.sort_by(f64::total_cmp);
    println!(
        "{name},{},{:.3},{:.3},{:.3},{:.3}",
        samples.len(),
        percentile(&samples, 50),
        percentile(&samples, 95),
        percentile(&samples, 99),
        samples[samples.len() - 1],
    );
}

fn tail_cases() {
    const SAMPLES: usize = 64;
    println!("tail,count,p50_us,p95_us,p99_us,max_us");

    let mut runtime = protocol_runtime(1, 1);
    let lane = runtime.lane(SWExecutionClass::High);
    let mut batch = lane.batch();
    let started = Arc::new(Mutex::new(None::<Instant>));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(SAMPLES)));
    let signal_started = Arc::clone(&started);
    let signal_samples = Arc::clone(&samples);
    let mut route = runtime
        .notification_route(move || {
            if let Some(start) = signal_started.lock().unwrap().take() {
                signal_samples
                    .lock()
                    .unwrap()
                    .push(start.elapsed().as_secs_f64() * 1e6);
            }
            Ok(())
        })
        .unwrap();
    for _ in 0..SAMPLES {
        let group = batch.begin().unwrap();
        let binding = route.watch_completion(&group.completion()).unwrap();
        route.prepare_wait().unwrap();
        *started.lock().unwrap() = Some(Instant::now());
        group.seal();
        drop(binding);
    }
    tail_row(
        "empty_group_seal_to_signal",
        std::mem::take(&mut *samples.lock().unwrap()),
    );
    route.close().unwrap();
    runtime.shutdown().unwrap();

    let mut runtime = protocol_runtime(1, 1);
    let phase = SWPhase(2);
    let mut owner = runtime
        .owner(0usize, std::num::NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let mut route = runtime.notification_route(|| Ok(())).unwrap();
    let _binding = route.watch_owner(&owner).unwrap();
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut current = owner.try_post(phase, |_| {}).unwrap().0;
    for _ in 0..SAMPLES {
        route.prepare_wait().unwrap();
        let start = Instant::now();
        owner.pump(phase, SWPumpBudget::new(1)).unwrap();
        assert!(current.status().is_settled());
        current = owner.try_post(phase, |_| {}).unwrap().0;
        samples.push(start.elapsed().as_secs_f64() * 1e6);
    }
    tail_row("owner_pump_to_capacity_reuse", samples);
    owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    route.close().unwrap();
    owner.close();
    runtime.shutdown().unwrap();

    let mut runtime = protocol_runtime(1, 1);
    let lane = runtime.lane(SWExecutionClass::High);
    let mut route = runtime.notification_route(|| Ok(())).unwrap();
    let _binding = route.watch_progress().unwrap();
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (successor_tx, successor_rx) = mpsc::channel();
        let started = Arc::new(Mutex::new(None::<Instant>));
        let successor_started = Arc::clone(&started);
        let (predecessor, _) = lane
            .try_spawn(SWSpawnOptions::default(), move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            })
            .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (successor, _) = lane
            .try_spawn_after(
                SWSpawnOptions::default(),
                &[predecessor.completion()],
                SWDependencyPolicy::SuccessOnly,
                move || {
                    successor_tx
                        .send(successor_started.lock().unwrap().unwrap().elapsed())
                        .unwrap();
                },
            )
            .unwrap();
        route.prepare_wait().unwrap();
        *started.lock().unwrap() = Some(Instant::now());
        release_tx.send(()).unwrap();
        samples.push(
            successor_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .as_secs_f64()
                * 1e6,
        );
        successor
            .completion()
            .wait_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    }
    tail_row("predecessor_release_to_successor_start", samples);
    route.close().unwrap();
    runtime.shutdown().unwrap();
}

fn protocol_runtime(routes: usize, bindings: usize) -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let owned = SWOwnedLimits::new(128, 128, [128; 3], [1; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(owned)
        .with_notification_limits(SWNotifyLimits { routes, bindings })
        .build()
        .unwrap()
}

fn protocol_cases() {
    const CHANGES: usize = 64;
    let phase = SWPhase(1);
    println!("protocol,sources,routes,changes,signals,elapsed_us");

    let mut runtime = protocol_runtime(4, 4);
    let mut owner = runtime
        .owner(0usize, std::num::NonZeroUsize::new(CHANGES).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let signals = Arc::new(AtomicUsize::new(0));
    let mut routes = Vec::new();
    let mut bindings = Vec::new();
    for _ in 0..4 {
        let counter = Arc::clone(&signals);
        let mut route = runtime
            .notification_route(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap();
        bindings.push(route.watch_owner(&owner).unwrap());
        route.prepare_wait().unwrap();
        routes.push(route);
    }
    let before = signals.load(Ordering::Relaxed);
    let started = Instant::now();
    owner.try_post(phase, |_| {}).unwrap();
    let elapsed = started.elapsed().as_secs_f64() * 1e6;
    println!(
        "fanout,1,4,1,{},{}",
        signals.load(Ordering::Relaxed) - before,
        elapsed
    );
    owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    for route in &mut routes {
        route.close().unwrap();
    }
    drop(bindings);
    owner.close();
    runtime.shutdown().unwrap();

    let mut runtime = protocol_runtime(1, 1);
    let mut owner = runtime
        .owner(0usize, std::num::NonZeroUsize::new(CHANGES).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let signals = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&signals);
    let mut route = runtime
        .notification_route(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .unwrap();
    let _binding = route.watch_owner(&owner).unwrap();
    route.prepare_wait().unwrap();
    let before = signals.load(Ordering::Relaxed);
    let started = Instant::now();
    for _ in 0..CHANGES {
        owner.try_post(phase, |_| {}).unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64() * 1e6;
    println!(
        "delayed_receiver,1,1,{CHANGES},{},{elapsed:.3}",
        signals.load(Ordering::Relaxed) - before,
    );
    assert_eq!(
        owner
            .pump(phase, SWPumpBudget::new(CHANGES))
            .unwrap()
            .invoked,
        CHANGES
    );
    let before = signals.load(Ordering::Relaxed);
    let started = Instant::now();
    for _ in 0..CHANGES {
        route.prepare_wait().unwrap();
        owner.try_post(phase, |_| {}).unwrap();
        owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64() * 1e6;
    println!(
        "rapid_rearm,1,1,{CHANGES},{},{elapsed:.3}",
        signals.load(Ordering::Relaxed) - before,
    );
    route.close().unwrap();
    owner.close();
    runtime.shutdown().unwrap();

    let mut runtime = protocol_runtime(1, 4);
    let mut owners = (0..4)
        .map(|_| {
            runtime
                .owner(0usize, std::num::NonZeroUsize::new(1).unwrap())
                .unwrap()
        })
        .collect::<Vec<_>>();
    for owner in &mut owners {
        owner.set_phase(phase).unwrap();
    }
    let signals = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&signals);
    let mut route = runtime
        .notification_route(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .unwrap();
    let _bindings = owners
        .iter()
        .map(|owner| route.watch_owner(owner).unwrap())
        .collect::<Vec<_>>();
    route.prepare_wait().unwrap();
    let before = signals.load(Ordering::Relaxed);
    let started = Instant::now();
    for owner in &mut owners {
        owner.try_post(phase, |_| {}).unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64() * 1e6;
    println!(
        "many_source_coalesced,4,1,4,{},{elapsed:.3}",
        signals.load(Ordering::Relaxed) - before,
    );
    route.close().unwrap();
    for owner in &mut owners {
        owner.close();
    }
    runtime.shutdown().unwrap();
}
