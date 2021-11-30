use std::{
    cmp::Reverse,
    collections::{BTreeMap, VecDeque},
    fmt,
    ops::DerefMut,
};

/// A type that can be put in a priority queue, tells the queue which order the items should come out in
pub trait Prioritised: Sized {
    /// Type of the priority, the higher prioritys are those which are larger based on [`Ord::cmp`].
    type Priority: Ord + Copy + Send;

    /// Get the priority of this thing
    fn priority(&self) -> Self::Priority;
}

/// A priority queue which:
/// * preserves insert-order of items at the same priority
/// * provides an iterator to partially drain according to a predicate
///
/// Could:
/// * provide priority along with the value like a Map key
pub struct PriorityQueue<T: Prioritised> {
    map: BTreeMap<Reverse<T::Priority>, VecDeque<T>>,
}

impl<T: Prioritised> Default for PriorityQueue<T> {
    fn default() -> Self {
        PriorityQueue::new()
    }
}

impl<T: Prioritised> PriorityQueue<T> {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn enqueue(&mut self, item: T) -> &T {
        let deque = self.map.entry(Reverse(item.priority())).or_default();
        deque.push_back(item);
        deque.back().unwrap()
    }

    pub fn dequeue(&mut self, mut idx: usize) -> Option<T> {
        for queue in self.map.values_mut() {
            if let Some(next) = queue.remove(idx) {
                return Some(next);
            } else {
                idx -= queue.len();
            }
            // we could have an `else` clause here to remove the empty sub-queue, but it's expected that a few priority levels will be used and so it's better to leave the m and avoid the allocations
        }
        None
    }

    pub fn get(&self, mut idx: usize) -> Option<&T> {
        for queue in self.map.values() {
            if let Some(item) = queue.get(idx) {
                return Some(item);
            } else {
                idx -= queue.len();
            }
        }
        None
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            buckets: self.map.iter_mut(),
            bucket: None,
        }
    }

    /// drains each element iterated, once the iterator is dropped, unlike `drain` implementations in the standard library, any remaining items are left in the queue
    pub fn drain_where<P: FnMut(&T) -> bool>(
        &mut self,
        predicate: P,
    ) -> DrainWhere<T, P, &mut Self> {
        Self::drain_where_deref(self, predicate)
    }

    /// drains each element iterated, once the iterator is dropped, unlike `drain` implementations in the standard library, any remaining items are left in the queue
    /// This version allows different receiver types, so it can be called on eg `MutexGuard<Self>` and then take ownership of the guard
    pub fn drain_where_deref<Q: DerefMut<Target = Self>, P: FnMut(&T) -> bool>(
        this: Q,
        predicate: P,
    ) -> DrainWhere<T, P, Q> {
        DrainWhere {
            queue: this,
            predicate,
            skip: 0,
        }
    }

    /// drains each element iterated, once the iterator is dropped, unlike `drain` implementations in the standard library, any remaining items are left in the queue
    pub fn drain(&mut self) -> DrainWhere<T, impl FnMut(&T) -> bool, &mut Self> {
        Self::drain_where_deref(self, |_| true)
    }

    /// drains each element iterated, once the iterator is dropped, unlike `drain` implementations in the standard library, any remaining items are left in the queue
    /// This version allows different receiver types, so it can be called on eg `MutexGuard<Self>` and then take ownership of the guard
    pub fn drain_deref<Q: DerefMut<Target = Self>>(
        this: Q,
    ) -> DrainWhere<T, impl FnMut(&T) -> bool, Q> {
        Self::drain_where_deref(this, |_| true)
    }

    pub fn is_empty(&self) -> bool {
        self.map.iter().all(|(_, queue)| queue.is_empty())
    }

    pub fn len(&self) -> usize {
        self.map.iter().map(|(_, queue)| queue.len()).sum()
    }
}

impl<T: Prioritised> fmt::Debug for PriorityQueue<T>
where
    <T as Prioritised>::Priority: fmt::Debug,
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(
                self.map
                    .iter()
                    .filter_map(|(Reverse(p), v)| if v.is_empty() { None } else { Some((p, v)) }),
            )
            .finish()
    }
}

pub struct IterMut<'q, T: Prioritised> {
    buckets: std::collections::btree_map::IterMut<'q, Reverse<T::Priority>, VecDeque<T>>,
    bucket: Option<std::collections::vec_deque::IterMut<'q, T>>,
}

impl<'q, T: Prioritised> Iterator for IterMut<'q, T> {
    type Item = &'q mut T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(bucket) = &mut self.bucket {
                if let Some(next) = bucket.next() {
                    return Some(next);
                } else {
                    self.bucket = None;
                }
            }
            if let Some((_priority, bucket)) = self.buckets.next() {
                self.bucket = Some(bucket.iter_mut());
            } else {
                return None;
            }
        }
    }
}

pub struct DrainWhere<T: Prioritised, P: FnMut(&T) -> bool, Q: DerefMut<Target = PriorityQueue<T>>>
{
    queue: Q,
    predicate: P,
    skip: usize,
}

impl<T: Prioritised, P: FnMut(&T) -> bool, Q: DerefMut<Target = PriorityQueue<T>>> Iterator
    for DrainWhere<T, P, Q>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(next) = self.queue.get(self.skip) {
                if (self.predicate)(next) {
                    return self.queue.dequeue(self.skip);
                } else {
                    self.skip += 1;
                }
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
    struct TestPriority(u8);

    #[derive(PartialEq, Eq, Debug)]
    struct PrioritisedJob(u8, char);

    impl Prioritised for PrioritisedJob {
        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0
        }
    }

    #[test]
    fn priority_queue_elements_come_out_prioritised_and_in_order() {
        let mut queue = PriorityQueue::new();
        queue.enqueue(PrioritisedJob(2, 'a'));
        queue.enqueue(PrioritisedJob(2, 'b'));
        queue.enqueue(PrioritisedJob(1, 'd'));
        queue.enqueue(PrioritisedJob(1, 'e'));
        queue.enqueue(PrioritisedJob(2, 'c'));
        let vals: String = queue.drain().map(|j| j.1).collect();
        assert_eq!(vals, "abcde");
    }

    #[test]
    fn drain_skip() {
        let mut queue = PriorityQueue::new();
        queue.enqueue(PrioritisedJob(1, 'a'));
        queue.enqueue(PrioritisedJob(1, 'b'));
        queue.enqueue(PrioritisedJob(1, 'c'));
        let mut drain = queue.drain_where(|i| i.1 == 'b');
        let next = drain.next().unwrap();
        assert_eq!(next, PrioritisedJob(1, 'b'));
        assert_eq!(drain.next(), None);

        let vals: String = queue.drain().map(|j| j.1).collect();
        assert_eq!(vals, "ac");

        assert_eq!(queue.drain().map(|j| j.1).count(), 0);
    }

    #[derive(PartialEq, Eq, Debug)]
    struct MergableJob(u8, char);

    impl PrioritisedMergeable for MergableJob {
        type Priority = TestPriority;

        fn priority(&self) -> Self::Priority {
            TestPriority(self.0)
        }

        fn merge(self, other: &mut MergableJob) -> MergeResult<MergableJob> {
            if self.1 == other.1 {
                other.0 = other.0.max(self.0);
                MergeResult::Success
            } else {
                MergeResult::NotMerged(self)
            }
        }
    }

    #[test]
    fn priority_queue_elements_are_merged() {
        let mut queue = PriorityQueue::new();
        queue.enqueue_merge(MergableJob(2, 'a'));
        queue.enqueue_merge(MergableJob(1, 'a'));
        queue.enqueue_merge(MergableJob(1, 'b'));
        queue.enqueue_merge(MergableJob(2, 'b'));
        queue.enqueue_merge(MergableJob(1, 'e'));
        queue.enqueue_merge(MergableJob(1, 'f'));
        queue.enqueue_merge(MergableJob(1, 'd'));
        queue.enqueue_merge(MergableJob(2, 'c'));
        queue.enqueue_merge(MergableJob(2, 'd'));
        let vals: String = queue.drain().map(|j| j.1).collect();
        assert_eq!(vals, "abcdef");
    }
}
