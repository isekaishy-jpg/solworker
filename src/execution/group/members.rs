//! Safe reusable membership metadata. Owning payloads stay in job controls.

use crate::scheduler::storage::{JobHandle, JobWeak};

pub(super) const PAGE_SLOTS: usize = 64;

#[cfg(test)]
#[path = "../../../tests/unit/group_membership.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MembershipKey {
    group: u64,
    incarnation: u64,
    slot: usize,
    generation: u64,
    job: u64,
}

struct Slot {
    generation: u64,
    job: Option<JobWeak>,
    previous: Option<usize>,
    next: Option<usize>,
    ready: bool,
    reservation_next: Option<usize>,
}

impl Slot {
    fn empty() -> Self {
        Self {
            generation: 0,
            job: None,
            previous: None,
            next: None,
            ready: false,
            reservation_next: None,
        }
    }
}

pub(super) struct Page {
    slots: [Slot; PAGE_SLOTS],
    free: u64,
    // A reservation pins its concrete page before admission can commit.
    in_use: usize,
}

impl Page {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| Slot::empty()),
            free: u64::MAX,
            in_use: 0,
        }
    }
}

struct DirectoryEntry {
    page: Option<Box<Page>>,
    incarnation: u64,
    vacant_next: Option<usize>,
    available_previous: Option<usize>,
    available_next: Option<usize>,
}

pub(super) struct Members {
    pages: Vec<DirectoryEntry>,
    next_incarnation: Option<u64>,
    vacant: Option<usize>,
    vacant_count: usize,
    available_pages: Option<usize>,
    available: usize,
    pub(super) reserved: usize,
    occupied: usize,
    head: Option<usize>,
    tail: Option<usize>,
}

pub(super) struct Growth {
    pages: Vec<Box<Page>>,
    directory: Vec<DirectoryEntry>,
}

impl Growth {
    pub(super) fn new(pages: usize, directory: usize) -> Self {
        Self {
            pages: (0..pages).map(|_| Box::new(Page::new())).collect(),
            directory: Vec::with_capacity(directory),
        }
    }
}

impl Members {
    pub(super) fn new() -> Self {
        Self {
            pages: Vec::new(),
            next_incarnation: Some(1),
            vacant: None,
            vacant_count: 0,
            available_pages: None,
            available: 0,
            reserved: 0,
            occupied: 0,
            head: None,
            tail: None,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    pub(super) fn reserve(&mut self, count: usize) -> Option<Option<usize>> {
        if self.available < count {
            return None;
        }
        let mut head = None;
        for _ in 0..count {
            let page_index = self.available_pages.expect("available page exists");
            let page = self.pages[page_index]
                .page
                .as_mut()
                .expect("available page occupied");
            let offset = page.free.trailing_zeros() as usize;
            let index = page_index * PAGE_SLOTS + offset;
            page.free &= !(1_u64 << offset);
            page.in_use += 1;
            page.slots[offset].reservation_next = head;
            head = Some(index);
            if page.free == 0 {
                self.remove_available(page_index);
            }
        }
        self.available -= count;
        self.reserved += count;
        Some(head)
    }

    pub(super) fn growth(&self, count: usize) -> (usize, usize) {
        let pages = count.saturating_sub(self.available).div_ceil(PAGE_SLOTS);
        let required = self.pages.len() + pages.saturating_sub(self.vacant_count);
        // Directory growth is geometric, while slot pages remain fixed-size.
        // Existing directory capacity and holes need no cold backing allocation.
        let directory = if self.pages.capacity() < required {
            required.max(self.pages.capacity().saturating_mul(2))
        } else {
            0
        };
        (pages, directory)
    }

    pub(super) fn install(&mut self, growth: &mut Growth, count: usize) {
        let needed = count.saturating_sub(self.available).div_ceil(PAGE_SLOTS);
        let directory = self.pages.len() + needed.saturating_sub(self.vacant_count);
        if needed == 0
            || growth.pages.len() < needed
            || (self.pages.capacity() < directory && growth.directory.capacity() < directory)
        {
            return;
        }
        if self.pages.capacity() < directory {
            growth.directory.append(&mut self.pages);
            std::mem::swap(&mut growth.directory, &mut self.pages);
        }
        for _ in 0..needed {
            let incarnation = self
                .next_incarnation
                .expect("group page identity exhausted");
            self.next_incarnation = incarnation.checked_add(1);
            let page = growth.pages.pop().expect("prepared growth page");
            let entry = DirectoryEntry {
                page: Some(page),
                incarnation,
                vacant_next: None,
                available_previous: None,
                available_next: None,
            };
            let index = if let Some(index) = self.vacant {
                self.vacant = self.pages[index].vacant_next;
                self.vacant_count -= 1;
                debug_assert!(self.pages[index].page.is_none());
                self.pages[index] = entry;
                index
            } else {
                let index = self.pages.len();
                self.pages.push(entry);
                index
            };
            self.available += PAGE_SLOTS;
            self.add_available(index);
        }
    }

    fn slot(&self, index: usize) -> &Slot {
        &self.pages[index / PAGE_SLOTS]
            .page
            .as_ref()
            .expect("live membership page")
            .slots[index % PAGE_SLOTS]
    }

    fn slot_mut(&mut self, index: usize) -> &mut Slot {
        &mut self.pages[index / PAGE_SLOTS]
            .page
            .as_mut()
            .expect("live membership page")
            .slots[index % PAGE_SLOTS]
    }

    fn add_available(&mut self, index: usize) {
        let next = self.available_pages;
        let entry = &mut self.pages[index];
        debug_assert!(entry.available_previous.is_none() && entry.available_next.is_none());
        entry.available_next = next;
        if let Some(next) = next {
            self.pages[next].available_previous = Some(index);
        }
        self.available_pages = Some(index);
    }

    fn remove_available(&mut self, index: usize) {
        let previous = self.pages[index].available_previous;
        let next = self.pages[index].available_next;
        if let Some(previous) = previous {
            self.pages[previous].available_next = next;
        } else {
            debug_assert_eq!(self.available_pages, Some(index));
            self.available_pages = next;
        }
        if let Some(next) = next {
            self.pages[next].available_previous = previous;
        }
        self.pages[index].available_previous = None;
        self.pages[index].available_next = None;
    }

    fn matches(&self, group: u64, key: MembershipKey) -> bool {
        if key.group != group {
            return false;
        }
        let Some(entry) = self.pages.get(key.slot / PAGE_SLOTS) else {
            return false;
        };
        if entry.incarnation != key.incarnation {
            return false;
        }
        let Some(page) = &entry.page else {
            return false;
        };
        let slot = &page.slots[key.slot % PAGE_SLOTS];
        slot.generation == key.generation
            && slot.job.as_ref().is_some_and(|job| job.id() == key.job)
    }

    pub(super) fn register(
        &mut self,
        group: u64,
        index: usize,
        job: JobWeak,
    ) -> (MembershipKey, Option<usize>) {
        assert!(self.reserved > 0, "membership capacity was reserved");
        self.reserved -= 1;
        self.occupied += 1;
        let incarnation = self.pages[index / PAGE_SLOTS].incarnation;
        let slot = self.slot_mut(index);
        let next = slot.reservation_next.take();
        slot.generation = slot
            .generation
            .checked_add(1)
            .expect("reserved slot is not exhausted");
        let key = MembershipKey {
            group,
            incarnation,
            slot: index,
            generation: slot.generation,
            job: job.id(),
        };
        slot.job = Some(job);
        (key, next)
    }

    pub(super) fn ready(&mut self, group: u64, key: MembershipKey) {
        if !self.matches(group, key) || self.slot(key.slot).ready {
            return;
        }
        let tail = self.tail;
        let slot = self.slot_mut(key.slot);
        slot.ready = true;
        slot.previous = tail;
        slot.next = None;
        if let Some(tail) = tail {
            self.slot_mut(tail).next = Some(key.slot);
        } else {
            self.head = Some(key.slot);
        }
        self.tail = Some(key.slot);
    }

    pub(super) fn unready(&mut self, group: u64, key: MembershipKey) {
        if !self.matches(group, key) || !self.slot(key.slot).ready {
            return;
        }
        let previous = self.slot(key.slot).previous;
        let next = self.slot(key.slot).next;
        if let Some(previous) = previous {
            self.slot_mut(previous).next = next;
        } else {
            self.head = next;
        }
        if let Some(next) = next {
            self.slot_mut(next).previous = previous;
        } else {
            self.tail = previous;
        }
        let slot = self.slot_mut(key.slot);
        slot.ready = false;
        slot.previous = None;
        slot.next = None;
    }

    fn release_slot(&mut self, index: usize) -> Option<Box<Page>> {
        let page_index = index / PAGE_SLOTS;
        let page = self.pages[page_index]
            .page
            .as_mut()
            .expect("released page exists");
        let was_full = page.free == 0;
        if page.slots[index % PAGE_SLOTS].generation != u64::MAX {
            page.free |= 1_u64 << (index % PAGE_SLOTS);
            self.available += 1;
        }
        page.in_use -= 1;
        let empty = page.in_use == 0;
        let now_available = page.free != 0;
        if was_full && now_available {
            self.add_available(page_index);
        }
        if empty && page_index != 0 {
            if now_available {
                self.remove_available(page_index);
            }
            let entry = &mut self.pages[page_index];
            let page = entry.page.take().expect("empty page exists");
            self.available -= page.free.count_ones() as usize;
            entry.vacant_next = self.vacant;
            self.vacant = Some(page_index);
            self.vacant_count += 1;
            return Some(page);
        }
        None
    }

    pub(super) fn release_reserved(&mut self, index: usize) -> (Option<usize>, Option<Box<Page>>) {
        debug_assert!(self.slot(index).job.is_none());
        let next = self.slot_mut(index).reservation_next.take();
        self.reserved -= 1;
        let removed = self.release_slot(index);
        (next, removed)
    }

    pub(super) fn retire(
        &mut self,
        group: u64,
        key: MembershipKey,
    ) -> (Option<JobWeak>, Option<Box<Page>>) {
        if !self.matches(group, key) {
            return (None, None);
        }
        self.unready(group, key);
        let removed = self.slot_mut(key.slot).job.take();
        self.occupied -= 1;
        let page = self.release_slot(key.slot);
        (removed, page)
    }

    pub(super) fn candidate(&mut self, group: u64) -> Option<JobHandle> {
        while let Some(index) = self.head {
            let slot = self.slot(index);
            let job = slot.job.as_ref().expect("ready member has an association");
            if let Some(job) = job.upgrade() {
                return Some(job);
            }
            let key = MembershipKey {
                group,
                incarnation: self.pages[index / PAGE_SLOTS].incarnation,
                slot: index,
                generation: slot.generation,
                job: job.id(),
            };
            self.unready(group, key);
        }
        None
    }

    pub(super) fn needs_trim(&self) -> bool {
        self.occupied == 0 && self.reserved == 0 && self.pages.len() > 1
    }

    pub(super) fn trim(&mut self, replacement: &mut Self) {
        if !self.needs_trim() {
            return;
        }
        let mut anchor = std::mem::replace(
            &mut self.pages[0],
            DirectoryEntry {
                page: None,
                incarnation: 0,
                vacant_next: None,
                available_previous: None,
                available_next: None,
            },
        );
        debug_assert!(anchor.page.as_ref().is_some_and(|page| page.in_use == 0));
        replacement.next_incarnation = self.next_incarnation;
        replacement.available = anchor
            .page
            .as_ref()
            .expect("anchor page remains")
            .free
            .count_ones() as usize;
        replacement.available_pages = (replacement.available != 0).then_some(0);
        anchor.available_previous = None;
        anchor.available_next = None;
        replacement.pages.push(anchor);
        // Every non-anchor Box was released at its own page's final member.
        // The retired directory contains only vacant metadata, never a burst
        // of payload-sized page allocations at final group settlement.
        debug_assert!(self.pages.iter().all(|entry| entry.page.is_none()));
        std::mem::swap(self, replacement);
    }

    pub(super) fn trim_storage() -> Self {
        let mut replacement = Self::new();
        replacement.pages = Vec::with_capacity(1);
        replacement
    }
}
