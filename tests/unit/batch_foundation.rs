use super::{SWCost, SWLimits, SWReservationError, SWReservationPool};

#[test]
fn ordinary_portions_shrink_and_members_release_independently() {
    let limits = SWLimits::new(SWCost::new(5, 7, 0, 0), SWCost::new(1, 1, 0, 0), 1, None).unwrap();
    let pool = SWReservationPool::new(1, limits);
    let per_member = SWCost::new(1, 2, 0, 0);
    let mut members = pool.try_reserve_ordinary_many(per_member, 64).unwrap();
    assert_eq!(members.len(), 3);
    assert_eq!(pool.snapshot().ordinary, SWCost::new(3, 6, 0, 0));
    assert!(matches!(
        pool.try_reserve_ordinary_many(per_member, 64),
        Err(SWReservationError::Full)
    ));

    // A surviving sibling must not pin the retired member's capacity.
    drop(members.remove(1));
    assert_eq!(pool.snapshot().ordinary, SWCost::new(2, 4, 0, 0));
    let replacement = pool.try_reserve_ordinary_many(per_member, 64).unwrap();
    assert_eq!(replacement.len(), 1);
    drop(members);
    assert_eq!(pool.snapshot().ordinary, per_member);
    drop(replacement);
    assert_eq!(pool.snapshot().ordinary, SWCost::default());
}

#[test]
fn ordinary_portion_checks_products_and_combined_byte_ceiling() {
    for hard_ceiling in [None, Some(usize::MAX)] {
        let limits = SWLimits::new(
            SWCost::new(64, usize::MAX, 0, usize::MAX),
            SWCost::new(1, 0, 0, 0),
            1,
            hard_ceiling,
        )
        .unwrap();
        let pool = SWReservationPool::new(2, limits);
        let required = pool
            .try_reserve_required(SWCost::new(1, 0, 0, usize::MAX / 2))
            .unwrap();
        let cost = SWCost::new(1, usize::MAX / 3, 0, usize::MAX / 3);
        let ordinary = pool.try_reserve_ordinary_many(cost, 64).unwrap();
        assert_eq!(ordinary.len(), 1);
        assert_eq!(pool.snapshot().ordinary, cost);
        assert!(matches!(
            pool.try_reserve_ordinary_many(cost, 64),
            Err(SWReservationError::Full)
        ));
        drop(required);
        let remainder = pool.try_reserve_ordinary_many(cost, 64).unwrap();
        assert_eq!(remainder.len(), 2);
        assert_eq!(pool.snapshot().ordinary.edges, (usize::MAX / 3) * 3);
        drop(ordinary);
        drop(remainder);
        assert_eq!(pool.snapshot().ordinary, SWCost::default());
    }
}

#[test]
fn ordinary_portion_classifies_individual_oversize_and_empty_closed_noop() {
    let limits = SWLimits::new(SWCost::new(1, 1, 0, 0), SWCost::default(), 0, None).unwrap();
    let pool = SWReservationPool::new(3, limits);
    assert!(matches!(
        pool.try_reserve_ordinary_many(SWCost::new(1, 2, 0, 0), 64),
        Err(SWReservationError::TooLarge)
    ));
    assert_eq!(pool.snapshot().ordinary, SWCost::default());
    pool.close();
    assert!(
        pool.try_reserve_ordinary_many(SWCost::new(1, 2, 0, 0), 0)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        pool.try_reserve_ordinary_many(SWCost::new(1, 0, 0, 0), 64),
        Err(SWReservationError::Closed)
    ));
}
