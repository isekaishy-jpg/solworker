//! A single-thread host combines a retained CPU group with its own wake token.

use std::thread;
use std::time::Duration;

use solworker::{
    SWExecutionClass, SWNotifyLimits, SWOutcome, SWOwnedLimits, SWRuntime, SWRuntimeConfig,
    SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};

fn main() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(8, 8, [8; 3], [1; 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(limits)
        .with_notification_limits(SWNotifyLimits {
            routes: 1,
            bindings: 1,
        })
        .build()
        .unwrap();

    let lane = runtime.lane(SWExecutionClass::Low);
    let mut batch = lane.batch();
    let host = thread::current();
    let mut route = runtime
        .notification_route(move || {
            host.unpark();
            Ok(())
        })
        .unwrap();
    for wave in 0..3_u32 {
        let group = batch.begin().unwrap();
        let completion = group.completion();
        let binding = route.watch_completion(&completion).unwrap();
        let mut tasks = Vec::new();
        let mut admission_error = None;
        for chunk in 0..4_u32 {
            let input = wave * 10 + chunk;
            match lane.try_spawn_in(group, SWSpawnOptions::default(), move || input * input) {
                Ok((task, _control)) => tasks.push(task),
                Err(rejected) => {
                    admission_error = Some(rejected.reason);
                    break;
                }
            }
        }
        group.seal(); // Also required when only some chunks were admitted.

        // Prepare host-owned presentation state while the CPU wave runs.
        let label = format!("wave {wave}");
        while completion.status().is_none() {
            // `park_timeout(0)` consumes an earlier unpark token. A native
            // loop drains or resets its event according to that contract.
            thread::park_timeout(Duration::ZERO);
            let stamp = route.prepare_wait().unwrap();
            if completion.status().is_some() {
                break;
            }
            if group.help_ready().unwrap() || route.changed_since(stamp).unwrap() {
                continue;
            }
            thread::park_timeout(Duration::from_millis(16));
        }

        assert_eq!(completion.status(), Some(SWTaskStatus::Succeeded));
        if let Some(reason) = admission_error {
            panic!("{label}: chunk admission rejected: {reason:?}");
        }
        let mut sum = 0_u32;
        for mut task in tasks {
            let Some(SWOutcome::Success(value)) = task.try_take() else {
                panic!("{label}: admitted chunk had no successful outcome");
            };
            sum += value;
        }
        println!("{label}: sum of four prepared chunks = {sum}");
        drop(binding); // The next wave has a distinct completion identity.
    }

    route.close().unwrap();
    while !route.is_quiescent() {
        thread::park_timeout(Duration::from_millis(1));
    }
    runtime.shutdown().unwrap();
}
