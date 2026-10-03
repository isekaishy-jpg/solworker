use super::*;

// Deliberately use a scanning reference model, independent of the indexes.
fn reference_pop(queue: &mut Vec<(u64, Option<DemandSelection>)>) -> Option<u64> {
    let ordinary = queue.iter().position(|(_, selection)| selection.is_none());
    let resource = queue
        .iter()
        .enumerate()
        .filter_map(|(index, (_, selection))| {
            selection.map(|s| (index, (!s.active, s.priority, s.tie)))
        })
        .min_by_key(|(_, key)| *key)
        .map(|(index, _)| index);
    let index = match (ordinary, resource) {
        (Some(a), Some(b)) if queue[a].0 < queue[b].0 => a,
        (_, Some(b)) => b,
        (Some(a), None) => a,
        (None, None) => return None,
    };
    Some(queue.remove(index).0)
}

#[test]
fn mixed_routes_match_scanning_reference_through_updates_and_removal() {
    let mut random = 0x3141_5926_u64;
    let mut next = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random
    };
    let mut queue = PendingQueue::default();
    let mut reference = Vec::new();
    for step in 0..20_000 {
        let id = next() % 257;
        match next() % 5 {
            0 | 1 if !reference.iter().any(|(queued, _)| *queued == id) => {
                let selection = (next() % 3 != 0).then(|| DemandSelection {
                    priority: Some(SWPriority::new((next() % 4) as u16)),
                    active: next() % 2 == 0,
                    tie: (next() % 11) as i128 - 5,
                    version: step,
                });
                if next() % 2 == 0 {
                    queue.push(id, selection);
                    reference.push((id, selection));
                } else {
                    queue.push_front(id, selection);
                    reference.insert(0, (id, selection));
                }
            }
            2 => {
                queue.remove(id);
                reference.retain(|(queued, _)| *queued != id);
            }
            3 => {
                if let Some(index) = reference
                    .iter()
                    .position(|(queued, s)| *queued == id && s.is_some())
                {
                    let (_, Some(mut selection)) = reference.remove(index) else {
                        unreachable!()
                    };
                    selection.active = !selection.active;
                    selection.priority = Some(SWPriority::new((next() % 4) as u16));
                    selection.tie = (next() % 11) as i128 - 5;
                    queue.update_resource(id, selection);
                    reference.push((id, Some(selection)));
                }
            }
            _ => assert_eq!(queue.pop(), reference_pop(&mut reference), "step {step}"),
        }
        assert_eq!(queue.is_empty(), reference.is_empty());
        assert_eq!(queue.entries.len(), reference.len());
        assert_eq!(queue.ordinary.len() + queue.resource.len(), reference.len());
    }
    while !reference.is_empty() {
        assert_eq!(queue.pop(), reference_pop(&mut reference));
    }
    assert_eq!(queue.pop(), None);
}
