use super::demand::{DemandCommand, DemandState, SWDemandError, SWPriority};
use super::ready::ReadyQueues;
use crate::runtime::config::SWExecutionClass;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn independent_consumers_keep_shared_producer_active() {
    let low = SWPriority::new(1);
    let background = SWPriority::new(5);
    let mut state = DemandState::new(vec![low, background], 2);
    state.register(1, Some(background), &[], None).unwrap();
    let first = state.attach(1, low).unwrap();
    let second = state.attach(1, low).unwrap();
    assert_eq!(state.attach(1, low), Err(SWDemandError::Full));
    state.service(4);
    assert_eq!(state.selection(1).unwrap().priority, Some(low));

    state.change(first, DemandCommand::Defer).unwrap();
    state.service(4);
    assert!(state.selection(1).unwrap().active);
    state.change(second, DemandCommand::Detach).unwrap();
    state.service(4);
    assert!(!state.selection(1).unwrap().active);
    state.change(first, DemandCommand::Refresh(low)).unwrap();
    state.service(4);
    assert!(state.selection(1).unwrap().active);
    state.change(first, DemandCommand::Detach).unwrap();
    state.service(4);
    assert_eq!(state.selection(1).unwrap().priority, Some(background));
}

#[test]
fn propagation_is_budgeted_and_detaches_settled_prerequisites() {
    let urgent = SWPriority::new(0);
    let normal = SWPriority::new(5);
    let mut state = DemandState::new(vec![urgent, normal], 2);
    state
        .register(1, None, &[], Some(Arc::new(|_| {})))
        .unwrap();
    state.register(2, None, &[1], None).unwrap();
    state.register(3, Some(normal), &[2], None).unwrap();
    state.service(8);
    let lease = state.attach(3, urgent).unwrap();
    assert!(state.pending_updates());
    state.service(1);
    assert!(state.pending_updates());
    state.service(8);
    assert_eq!(state.selection(1).unwrap().priority, Some(urgent));
    state.remove(2);
    let changes = state.service(8);
    assert_eq!(state.selection(1).unwrap().priority, None);
    let (_, snapshot) = changes
        .into_iter()
        .find(|change| change.id == 1)
        .unwrap()
        .provider
        .unwrap();
    assert_eq!(snapshot.priority, None);
    assert!(!snapshot.active);
    state.change(lease, DemandCommand::Detach).unwrap();
}

#[test]
fn resource_rank_and_explicit_tie_promotion_do_not_reorder_ordinary_fifo() {
    let urgent = SWPriority::new(0);
    let normal = SWPriority::new(5);
    let mut state = DemandState::new(vec![urgent, normal], 2);
    for id in [2, 3, 4] {
        state.register(id, Some(normal), &[], None).unwrap();
    }
    let lease = state.attach(4, normal).unwrap();
    state.change(lease, DemandCommand::Promote).unwrap();
    state.service(8);

    let mut ready = ReadyQueues::with_priorities(&[urgent, normal]);
    let class = SWExecutionClass::Mid;
    ready.push(class, 1);
    for id in [2, 3, 4] {
        ready.push_resource(class, id, state.selection(id).unwrap());
    }
    assert_eq!(ready.pop(class), Some(1));
    assert_eq!(ready.pop(class), Some(4));
    assert_eq!(ready.pop(class), Some(2));
    assert_eq!(ready.pop(class), Some(3));
}

#[test]
fn provider_snapshots_are_returned_without_invoking_the_hook() {
    let rank = SWPriority::new(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let hook = Arc::new(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
    });
    let mut state = DemandState::new(vec![rank], 1);
    state.register(1, Some(rank), &[], Some(hook)).unwrap();
    let mut changes = state.service(1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (_, snapshot) = changes.pop().unwrap().provider.unwrap();
    assert_eq!(snapshot.priority, Some(rank));
    assert!(snapshot.active);
}
