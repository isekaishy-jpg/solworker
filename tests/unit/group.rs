use super::GroupInner;
use crate::{SWExecutionClass, SWTaskStatus};
use std::sync::{Arc, mpsc};

#[test]
fn terminal_wake_precedes_subscriber_activation_even_when_it_unwinds() {
    for has_member in [false, true] {
        let group = Arc::new(GroupInner::new(1, SWExecutionClass::High));
        if has_member {
            assert!(group.add());
            group.seal();
        }
        let before = group.generation();
        let completion = group.completion();
        let observed = Arc::clone(&group);
        let (send, receive) = mpsc::channel();
        let _subscription = completion.subscribe_cancelable(Box::new(move |status| {
            // There must be a wake AFTER status publication, in addition to the
            // membership-change wake. No real thread scheduling is needed to
            // exercise the window where a waiter consumes the earlier wake.
            send.send((status, observed.generation().wrapping_sub(before)))
                .unwrap();
            panic!("downstream activation unwound");
        }));
        let activation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if has_member {
                group.finish(Some(SWTaskStatus::Succeeded));
            } else {
                group.seal();
            }
        }));
        assert!(activation.is_err());
        let (status, wakes) = receive.try_recv().unwrap();
        assert_eq!(status, SWTaskStatus::Succeeded);
        assert!(
            wakes >= 2,
            "terminal wake was delayed until after activation"
        );
        assert!(group.is_complete());
    }
}
