use super::{
    SWCapacityUsage, SWCost, SWLimitError, SWLimits, SWReservationError, SWReservationPool,
};

fn limits(ceiling: Option<usize>) -> SWLimits {
    SWLimits::new(
        SWCost::new(2, 2, 2, 100),
        SWCost::new(2, 2, 2, 0),
        1,
        ceiling,
    )
    .unwrap()
}

#[test]
fn ordinary_cannot_borrow_required_metadata_and_required_can_cross_byte_target() {
    let pool = SWReservationPool::new(7, limits(None));
    let ordinary = pool
        .try_reserve_ordinary(SWCost::new(2, 0, 0, 100))
        .unwrap();
    assert!(matches!(
        pool.try_reserve_ordinary(SWCost::new(1, 0, 0, 0)),
        Err(SWReservationError::Full)
    ));
    let required = pool
        .try_reserve_required(SWCost::new(2, 1, 1, 500))
        .unwrap();
    assert_eq!(required.runtime_identity(), 7);
    assert_eq!(pool.snapshot().required.bytes, 500);
    assert!(matches!(
        pool.try_reserve_required(SWCost::new(0, 0, 0, 1)),
        Err(SWReservationError::Full)
    ));
    drop(required);
    drop(ordinary);
    assert_eq!(
        pool.snapshot(),
        SWCapacityUsage {
            ordinary: SWCost::default(),
            required: SWCost::default(),
            required_pipelines: 0
        }
    );
}

#[test]
fn child_and_retained_bytes_outlive_pipeline_handle() {
    let pool = SWReservationPool::new(9, limits(Some(150)));
    let root = pool
        .try_reserve_required(SWCost::new(1, 1, 1, 125))
        .unwrap();
    let stage = root.stage(SWCost::new(1, 1, 1, 125)).unwrap();
    let retained = stage.retain_bytes(100).unwrap();
    drop(root);
    drop(stage);
    assert_eq!(pool.snapshot().required.bytes, 100);
    assert_eq!(pool.snapshot().required_pipelines, 0);
    assert!(matches!(
        pool.try_reserve_required(SWCost::new(1, 0, 0, 60)),
        Err(SWReservationError::Full)
    ));
    let next = pool.try_reserve_required(SWCost::new(1, 0, 0, 40)).unwrap();
    drop(next);
    drop(retained);
    assert_eq!(pool.snapshot().required.bytes, 0);
}

#[test]
fn stage_credits_return_after_nested_children_drop() {
    for drop_order in 0..3 {
        let pool = SWReservationPool::new(1, limits(None));
        let root = pool.try_reserve_required(SWCost::new(2, 2, 2, 20)).unwrap();
        let stage = root.stage(SWCost::new(2, 2, 2, 10)).unwrap();
        let first_output = stage.retain_bytes(5).unwrap();
        let second_output = stage.retain_bytes(5).unwrap();
        let child = stage.stage(SWCost::new(1, 1, 1, 0)).unwrap();
        match drop_order {
            0 => {
                drop(stage);
                drop(child);
            }
            1 => {
                drop(child);
                drop(stage);
            }
            _ => {
                let barrier = std::sync::Barrier::new(2);
                let barrier = &barrier;
                std::thread::scope(|inner| {
                    inner.spawn(move || {
                        barrier.wait();
                        drop(stage);
                    });
                    inner.spawn(move || {
                        barrier.wait();
                        drop(child);
                    });
                });
            }
        }
        assert_eq!(root.available(), SWCost::new(2, 2, 2, 10));
        drop(first_output);
        assert_eq!(root.available(), SWCost::new(2, 2, 2, 15));
        drop(second_output);
        assert_eq!(root.available(), SWCost::new(2, 2, 2, 20));
        assert_eq!(pool.snapshot().required.bytes, 20);
    }
}

#[test]
fn grow_failure_preserves_original_credits() {
    let pool = SWReservationPool::new(1, limits(Some(120)));
    let mut root = pool.try_reserve_required(SWCost::new(1, 0, 0, 80)).unwrap();
    let ordinary = pool.try_reserve_ordinary(SWCost::new(0, 0, 0, 30)).unwrap();
    let before = root.available();
    assert_eq!(
        root.try_grow(SWCost::new(0, 0, 0, 50)),
        Err(SWReservationError::TooLarge)
    );
    assert_eq!(root.available(), before);
    assert_eq!(pool.snapshot().required, before);
    assert_eq!(
        root.try_grow(SWCost::new(0, 0, 0, 20)),
        Err(SWReservationError::Full)
    );
    assert_eq!(root.available(), before);
    drop(ordinary);
    root.try_grow(SWCost::new(1, 0, 0, 20)).unwrap();
    assert_eq!(root.available(), SWCost::new(2, 0, 0, 100));
    assert_eq!(
        root.try_grow(SWCost::new(1, 0, 0, 0)),
        Err(SWReservationError::TooLarge)
    );
}

#[test]
fn invalid_limits_and_close() {
    assert_eq!(
        SWLimits::new(SWCost::default(), SWCost::default(), 1, None),
        Err(SWLimitError::ZeroRequiredCapacity)
    );
    assert_eq!(
        SWLimits::new(
            SWCost::new(0, 0, 0, 10),
            SWCost::new(1, 0, 0, 0),
            1,
            Some(9)
        ),
        Err(SWLimitError::OrdinaryTargetExceedsCeiling)
    );
    let pool = SWReservationPool::new(1, limits(None));
    pool.close();
    assert!(matches!(
        pool.try_reserve_required(SWCost::new(1, 0, 0, 0)),
        Err(SWReservationError::Closed)
    ));
}
