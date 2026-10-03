use solworker::*;
use std::{cell::RefCell, time::Duration};

thread_local! {
    static PRODUCER: RefCell<Option<SWProducer<()>>> = const { RefCell::new(None) };
}

fn check_thread_exit(initialize_activation_first: bool, width: usize, depth: usize) {
    let mut runtime =
        SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
            .with_owned_limits(
                SWOwnedLimits::new(width * depth + 16, width * depth + 16, [8; 3], [4; 3]).unwrap(),
            )
            .build()
            .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let (producer, predecessor, _) = runtime
        .external::<()>(SWExternalOptions::default())
        .unwrap();
    let mut successors = Vec::new();
    let mut prerequisite = predecessor.completion();
    for _ in 0..depth {
        let members = lane
            .try_spawn_batch(
                SWBatchSpawnOptions {
                    prerequisites: &[prerequisite],
                    ..Default::default()
                },
                (0..width).map(|_| || ()).collect(),
            )
            .unwrap();
        prerequisite = members[0].0.completion();
        successors.extend(members);
    }
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || {
            let initialize = || {
                let ready = SWTask::ready(());
                let (task, _) = lane
                    .try_spawn_after(
                        SWSpawnOptions::default(),
                        &[ready.completion()],
                        SWDependencyPolicy::SuccessOnly,
                        || (),
                    )
                    .unwrap();
                assert_eq!(
                    task.completion()
                        .wait_timeout(Duration::from_secs(5))
                        .unwrap(),
                    Some(SWTaskStatus::Succeeded)
                );
            };
            if initialize_activation_first {
                initialize();
            }
            PRODUCER.with(|slot| *slot.borrow_mut() = Some(producer));
            if !initialize_activation_first {
                initialize();
            }
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(predecessor.status(), Some(SWTaskStatus::Abandoned));
    // SuccessOnly suppression is synchronous in the publishing thread; join
    // observes its complete teardown. No sleep establishes this ordering.
    let observed: Vec<_> = successors.iter().map(|(task, _)| task.status()).collect();
    if observed.iter().any(|status| status.is_none()) {
        runtime.abandon();
    }
    assert_eq!(
        observed,
        vec![Some(SWTaskStatus::PrerequisiteFailed); width * depth]
    );
    runtime.shutdown().unwrap();
}

#[test]
fn producer_drop_at_thread_exit_settles_singleton() {
    check_thread_exit(false, 1, 1);
}

#[test]
fn producer_drop_at_thread_exit_settles_range() {
    check_thread_exit(false, 2, 1);
}

#[test]
fn positive_control_reversed_tls_initialization_settles() {
    check_thread_exit(true, 2, 1);
}

#[test]
fn tls_teardown_drains_long_ranges_iteratively() {
    check_thread_exit(false, 2, 768);
}
