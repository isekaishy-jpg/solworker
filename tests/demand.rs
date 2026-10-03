use std::sync::mpsc;
use std::time::Duration;

use solworker::{
    SWBatchSpawnOptions, SWExecutionClass, SWExternalOptions, SWOwnedLimits, SWPriority, SWRuntime,
    SWRuntimeConfig, SWSpawnOptions, SWStageOptions, SWTaskStatus, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(10);

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(32, 32, [16; 3], [1; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(limits)
        .with_demand_limits(
            vec![SWPriority::new(0), SWPriority::new(1), SWPriority::new(5)],
            8,
        )
        .build()
        .unwrap()
}

fn service_all(runtime: &SWRuntime) {
    for _ in 0..32 {
        if !runtime.service_demand(1) {
            return;
        }
    }
    panic!("bounded demand graph did not settle");
}

#[test]
fn resource_rank_and_promotion_order_pending_work_without_changing_ordinary_fifo() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Low);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (running, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();

    let (order_send, order_recv) = mpsc::channel();
    let ordinary_send = order_send.clone();
    let (ordinary, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            ordinary_send.send("ordinary").unwrap();
        })
        .unwrap();
    let first_send = order_send.clone();
    let (first, _) = lane
        .try_spawn_stage(
            SWStageOptions {
                priority: Some(SWPriority::new(1)),
                ..Default::default()
            },
            move || {
                first_send.send("first").unwrap();
            },
        )
        .unwrap();
    let promoted_send = order_send.clone();
    let (promoted, _) = lane
        .try_spawn_stage(
            SWStageOptions {
                priority: Some(SWPriority::new(1)),
                ..Default::default()
            },
            move || {
                promoted_send.send("promoted").unwrap();
            },
        )
        .unwrap();
    let demand = promoted.completion().demand(SWPriority::new(1)).unwrap();
    demand.promote().unwrap();

    // Demand producers and the next affected dispatch preserve ordering without
    // requiring another application pump or another ordinary submission.

    release_send.send(()).unwrap();
    let observed: Vec<_> = (0..3)
        .map(|_| order_recv.recv_timeout(TIMEOUT).unwrap())
        .collect();
    assert_eq!(observed, ["ordinary", "promoted", "first"]);
    for completion in [
        running.completion(),
        ordinary.completion(),
        first.completion(),
        promoted.completion(),
    ] {
        assert_eq!(
            completion.wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
    }
    runtime.shutdown().unwrap();
}

#[test]
fn independent_deferred_interest_yields_to_active_work_and_refresh_reactivates() {
    for reactivate in [false, true] {
        let mut runtime = runtime();
        let lane = runtime.lane(SWExecutionClass::Low);
        let (started_send, started_recv) = mpsc::channel();
        let (release_send, release_recv) = mpsc::channel();
        let (running, _) = lane
            .try_spawn(SWSpawnOptions::default(), move || {
                started_send.send(()).unwrap();
                release_recv.recv_timeout(TIMEOUT).unwrap();
            })
            .unwrap();
        started_recv.recv_timeout(TIMEOUT).unwrap();

        let (order_send, order_recv) = mpsc::channel();
        let shared_send = order_send.clone();
        let (shared, _) = lane
            .try_spawn_stage(
                SWStageOptions {
                    priority: Some(SWPriority::new(5)),
                    ..Default::default()
                },
                move || {
                    shared_send.send("shared").unwrap();
                },
            )
            .unwrap();
        let active_send = order_send.clone();
        let (active, _) = lane
            .try_spawn_stage(
                SWStageOptions {
                    priority: Some(SWPriority::new(1)),
                    ..Default::default()
                },
                move || {
                    active_send.send("active").unwrap();
                },
            )
            .unwrap();
        let completion = shared.completion();
        let urgent = completion.demand(SWPriority::new(0)).unwrap();
        let other = completion.demand(SWPriority::new(1)).unwrap();
        drop(other);
        urgent.defer().unwrap();
        if reactivate {
            urgent.refresh(SWPriority::new(0)).unwrap();
        }
        service_all(&runtime);

        release_send.send(()).unwrap();
        let observed: Vec<_> = (0..2)
            .map(|_| order_recv.recv_timeout(TIMEOUT).unwrap())
            .collect();
        let expected = if reactivate {
            ["shared", "active"]
        } else {
            ["active", "shared"]
        };
        assert_eq!(observed, expected);
        for completion in [
            running.completion(),
            shared.completion(),
            active.completion(),
        ] {
            assert_eq!(
                completion.wait_timeout(TIMEOUT).unwrap(),
                Some(SWTaskStatus::Succeeded)
            );
        }
        runtime.shutdown().unwrap();
    }
}

#[test]
fn consumer_demand_reaches_unresolved_resource_prerequisite() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Low);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (running, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();

    let (order_send, order_recv) = mpsc::channel();
    let parent_send = order_send.clone();
    let (parent, _) = lane
        .try_spawn_stage(
            SWStageOptions {
                priority: Some(SWPriority::new(5)),
                ..Default::default()
            },
            move || {
                parent_send.send("parent").unwrap();
            },
        )
        .unwrap();
    let peer_send = order_send.clone();
    let (peer, _) = lane
        .try_spawn_stage(
            SWStageOptions {
                priority: Some(SWPriority::new(1)),
                ..Default::default()
            },
            move || {
                peer_send.send("peer").unwrap();
            },
        )
        .unwrap();
    let prerequisites = [parent.completion()];
    let child_send = order_send.clone();
    let (child, _) = lane
        .try_spawn_stage(
            SWStageOptions {
                priority: Some(SWPriority::new(5)),
                prerequisites: &prerequisites,
                ..Default::default()
            },
            move || {
                child_send.send("child").unwrap();
            },
        )
        .unwrap();
    let _demand = child.completion().demand(SWPriority::new(0)).unwrap();

    release_send.send(()).unwrap();
    let observed: Vec<_> = (0..3)
        .map(|_| order_recv.recv_timeout(TIMEOUT).unwrap())
        .collect();
    assert_eq!(observed[0], "parent");
    assert_eq!(observed[2], "peer");
    for completion in [
        running.completion(),
        parent.completion(),
        peer.completion(),
        child.completion(),
    ] {
        assert_eq!(
            completion.wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
    }
    runtime.shutdown().unwrap();
}

#[test]
fn settling_ordinary_dependency_releases_provider_demand_without_another_pump() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Mid);
    let (advisory_send, advisory_recv) = mpsc::channel();
    let (producer, parent, _) = runtime
        .external::<()>(SWExternalOptions {
            provider_demand: Some(std::sync::Arc::new(move |snapshot| {
                advisory_send.send(snapshot).unwrap();
            })),
            ..Default::default()
        })
        .unwrap();
    let mut members = lane
        .try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &[parent.completion()],
                ..Default::default()
            },
            (0..2)
                .map(|index| {
                    move || {
                        assert_ne!(index, 0, "cancelled dependency must not execute");
                    }
                })
                .collect(),
        )
        .unwrap();
    let (ordinary, cancel) = members.remove(0);
    let prerequisites = [ordinary.completion()];
    let (resource, _) = lane
        .try_spawn_stage(
            SWStageOptions {
                prerequisites: &prerequisites,
                priority: Some(SWPriority::new(5)),
                ..Default::default()
            },
            || panic!("failed dependency must suppress its resource successor"),
        )
        .unwrap();
    let _demand = resource.completion().demand(SWPriority::new(0)).unwrap();
    // Read until the active advisory: registration may also publish an initial
    // empty snapshot. A deadline bounds failure without driving scheduler work.
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let snapshot = advisory_recv
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .unwrap();
        if snapshot.priority == Some(SWPriority::new(0)) && snapshot.active {
            break;
        }
    }

    cancel.cancel();
    assert_eq!(
        ordinary.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Cancelled)
    );
    assert_eq!(
        resource.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::PrerequisiteFailed)
    );
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let snapshot = advisory_recv
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .unwrap();
        if snapshot.priority.is_none() && !snapshot.active {
            break;
        }
    }
    assert_eq!(parent.status(), None);
    producer.complete(()).unwrap();
    assert_eq!(
        members[0].0.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
}
