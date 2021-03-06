use std::{
    cmp::Reverse,
    collections::{BTreeMap, VecDeque},
    fmt,
    ops::DerefMut,
};

use crate::{MergeResult, Prioritised};

use self::may_be_taken::SkipIterator;

pub(crate) struct PriorityQueue<T: Prioritised> {
    map: BTreeMap<Reverse<T::Priority>, VecDeque<T>>,
    merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
}

impl<T: Prioritised> Default for PriorityQueue<T> {
    fn default() -> Self {
        PriorityQueue::new(None)
    }
}

impl<T: Prioritised> PriorityQueue<T> {
    pub fn new(merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>) -> Self {
        Self {
            map: BTreeMap::new(),
            merge_fn,
        }
    }

    /// Enqueues the item so that it will be iterated before any existing items in the queue with a lower priority and after any existing items with the same or higher priority.
    /// If `T` has a merge function in `T::ATTEMPT_MERGE_INTO`, the item will be merged into the highest priority existing item which merges successfully. The queue should maintain a state where everything that can be merged is merged, as long as the merge function is transitive in it's successes.
    pub fn enqueue(&mut self, mut item: T) {
        if let Some(attempt_merge) = self.merge_fn {
            for (Reverse(priority), bucket) in &mut self.map {
                // for now we iterate over the whole queue to look for merges, not the best solution
                for (idx, existing) in bucket.iter_mut().enumerate() {
                    match (attempt_merge)(item, existing) {
                        MergeResult::NotMerged(the_item) => item = the_item,
                        MergeResult::Success => {
                            if &existing.priority() != priority {
                                let item = bucket.remove(idx).unwrap();
                                self.enqueue_internal(item);
                            }
                            return;
                        }
                    }
                }
            }
        }
        self.enqueue_internal(item);
    }

    pub fn enqueue_internal(&mut self, item: T) -> &T {
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

    /// drains each element iterated, once the iterator is dropped, *unlike `drain` implementations in the standard library, any remaining items are left in the queue
    pub fn drain(&mut self) -> Drain<T, &mut Self> {
        Self::drain_deref(self)
    }

    /// drains each element iterated, once the iterator is dropped, *unlike `drain` implementations in the standard library, any remaining items are left in the queue
    /// This version allows different receiver types, so it can be called on eg `MutexGuard<Self>` and then take ownership of the guard
    pub fn drain_deref<Q: DerefMut<Target = Self>>(this: Q) -> Drain<T, Q> {
        Drain {
            queue: this,
            skip: 0,
        }
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

pub(crate) struct Drain<T: Prioritised, Q: DerefMut<Target = PriorityQueue<T>>> {
    queue: Q,
    skip: usize,
}

impl<T: Prioritised, Q: DerefMut<Target = PriorityQueue<T>>> Iterator for Drain<T, Q> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.dequeue(self.skip)
    }
}

impl<T: Prioritised, Q: DerefMut<Target = PriorityQueue<T>>> SkipIterator for Drain<T, Q> {
    type Item = T;

    fn has_next(&self) -> bool {
        self.queue.len() > self.skip
    }

    fn peek(&self) -> Option<&Self::Item> {
        self.queue.get(self.skip)
    }

    fn take(&mut self) -> Option<Self::Item> {
        self.queue.dequeue(self.skip)
    }

    fn skip(&mut self) {
        self.skip += 1
    }
}

pub(crate) mod may_be_taken {
    use std::ops::Deref;

    /// Like an iterator, call `.maybe_next()` to get the next item. Unlike an iterator, only removes the item from the backing collection when calling `.into_inner()` on the returned `SkipableNext`. This way you can combine the behaviour of `.iter()` and `.into_iter()`, `SkipIterator` is equivalent to `.iter()` if `.into_inner()` is never called and equivalent to `into_iter()` if `into_inner()` is always called.
    pub trait SkipIterator {
        type Item;
        fn maybe_next(&mut self) -> Option<SkipableNext<'_, Self>> {
            if self.has_next() {
                Some(SkipableNext {
                    iter: self,
                    taken: false,
                })
            } else {
                None
            }
        }

        /// does the iterator have a next item, if true, `peek()` and `take()` must return `Some(T)`
        fn has_next(&self) -> bool;
        /// a reference to the next item
        fn peek(&self) -> Option<&Self::Item>;
        /// remove and return the next item
        fn take(&mut self) -> Option<Self::Item>;
        /// skip the next item
        fn skip(&mut self);
    }

    pub struct SkipableNext<'i, I: SkipIterator + ?Sized> {
        iter: &'i mut I,
        taken: bool,
    }

    impl<'i, I: SkipIterator + ?Sized> Drop for SkipableNext<'i, I> {
        fn drop(&mut self) {
            if !self.taken {
                self.iter.skip()
            }
        }
    }

    impl<'i, I> Deref for SkipableNext<'i, I>
    where
        I: SkipIterator,
    {
        type Target = I::Item;
        fn deref(&self) -> &I::Item {
            self.iter.peek().unwrap() // `SkipableNext` is only created after a `has_next()` check, so safe to unwrap
        }
    }

    impl<'i, I: SkipIterator> SkipableNext<'i, I> {
        pub fn into_inner(mut self) -> I::Item {
            self.taken = true;
            self.iter.take().unwrap() // `SkipableNext` is only created after a `has_next()` check, so safe to unwrap
        }
    }

    pub struct VecSkipIter<'v, T> {
        vec: &'v mut Vec<T>,
        skip: usize,
    }

    #[allow(dead_code)]
    impl<'v, T> VecSkipIter<'v, T> {
        pub fn new(vec: &'v mut Vec<T>) -> Self {
            Self { vec, skip: 0 }
        }
    }

    impl<'v, T> SkipIterator for VecSkipIter<'v, T> {
        type Item = T;

        fn has_next(&self) -> bool {
            self.vec.len() > self.skip
        }
        fn peek(&self) -> Option<&T> {
            self.vec.get(self.skip)
        }

        fn take(&mut self) -> Option<T> {
            if self.has_next() {
                Some(self.vec.remove(self.skip))
            } else {
                None
            }
        }

        fn skip(&mut self) {
            self.skip += 1;
        }
    }

    #[test]
    fn test() {
        let mut v = vec![1, 2, 3, 4];
        let mut iter = VecSkipIter::new(&mut v);
        let next = iter.maybe_next().unwrap();
        assert_eq!(*next, 1);
        assert_eq!(next.into_inner(), 1);
        let next = iter.maybe_next().unwrap();
        assert_eq!(*next, 2);
        drop(next);
        let next = iter.maybe_next().unwrap();
        assert_eq!(*next, 3);
        assert_eq!(next.into_inner(), 3);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::MergeResult;

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
        let mut queue = PriorityQueue::new(None);
        queue.enqueue(PrioritisedJob(2, 'a'));
        queue.enqueue(PrioritisedJob(2, 'b'));
        queue.enqueue(PrioritisedJob(1, 'd'));
        queue.enqueue(PrioritisedJob(1, 'e'));
        queue.enqueue(PrioritisedJob(2, 'c'));
        let vals: String = queue.drain().map(|j| j.1).collect();
        assert_eq!(vals, "abcde");
    }

    #[test]
    fn drain_peek() {
        let mut queue = PriorityQueue::new(None);
        queue.enqueue(PrioritisedJob(1, 'a'));
        queue.enqueue(PrioritisedJob(1, 'b'));
        let mut drain = queue.drain();
        let next = drain.maybe_next().unwrap();
        assert_eq!(*next, PrioritisedJob(1, 'a'));
        assert_eq!(next.into_inner(), PrioritisedJob(1, 'a'));
        let next = drain.maybe_next().unwrap();
        assert_eq!(*next, PrioritisedJob(1, 'b'));
        assert_eq!(next.into_inner(), PrioritisedJob(1, 'b'));
    }

    #[test]
    fn drain_skip() {
        let mut queue = PriorityQueue::new(None);
        queue.enqueue(PrioritisedJob(1, 'a'));
        queue.enqueue(PrioritisedJob(1, 'b'));
        queue.enqueue(PrioritisedJob(1, 'c'));
        let mut drain = queue.drain();
        assert_eq!(drain.maybe_next().as_deref(), Some(&PrioritisedJob(1, 'a')));
        {
            let next = drain.maybe_next().unwrap();
            assert_eq!(*next, PrioritisedJob(1, 'b'));
            assert_eq!(next.into_inner(), PrioritisedJob(1, 'b'));
        }
        assert_eq!(drain.maybe_next().as_deref(), Some(&PrioritisedJob(1, 'c')));
        assert_eq!(drain.maybe_next().as_deref(), None);

        let vals: String = queue.drain().map(|j| j.1).collect();
        assert_eq!(vals, "ac");

        assert_eq!(queue.drain().map(|j| j.1).count(), 0);
    }

    #[derive(PartialEq, Eq, Debug)]
    struct MergableJob(u8, char);

    impl Prioritised for MergableJob {
        type Priority = TestPriority;

        fn priority(&self) -> Self::Priority {
            TestPriority(self.0)
        }
    }

    fn merge(me: MergableJob, other: &mut MergableJob) -> MergeResult<MergableJob> {
        if me.1 == other.1 {
            other.0 = other.0.max(me.0);
            MergeResult::Success
        } else {
            MergeResult::NotMerged(me)
        }
    }

    #[test]
    fn priority_queue_elements_are_merged() {
        let mut queue = PriorityQueue::new(Some(merge));
        queue.enqueue(MergableJob(2, 'a'));
        queue.enqueue(MergableJob(1, 'a'));
        queue.enqueue(MergableJob(1, 'b'));
        queue.enqueue(MergableJob(2, 'b'));
        queue.enqueue(MergableJob(1, 'e'));
        queue.enqueue(MergableJob(1, 'f'));
        queue.enqueue(MergableJob(1, 'd'));
        queue.enqueue(MergableJob(2, 'c'));
        queue.enqueue(MergableJob(2, 'd'));
        let vals: String = queue.drain().map(|j| j.1).collect();
        assert_eq!(vals, "abcdef");
    }
}

pub(crate) mod prioritized_mpsc {
    use parking_lot::{Mutex, MutexGuard};
    use std::{fmt, sync::Arc, thread, time::Duration};

    use crate::{MergeResult, Prioritised};

    use super::PriorityQueue;

    pub(crate) struct Receiver<T: Prioritised> {
        queue: Arc<Mutex<PriorityQueue<T>>>,
        recv: crossbeam_channel::Receiver<T>,
    }

    impl<T: Prioritised> fmt::Debug for Receiver<T>
    where
        <T as Prioritised>::Priority: fmt::Debug,
        T: fmt::Debug,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.queue.fmt(f)
        }
    }

    impl<T: Prioritised> Receiver<T> {
        /// Processes things currently ready in the queue without blocking
        pub fn process_queue_ready(&mut self, mut cb: impl FnMut(&T)) -> bool {
            let mut has_new = false;
            let mut queue = self.queue.lock();
            for item in self.recv.try_iter() {
                cb(&item);
                queue.enqueue(item);
                has_new = true;
            }
            has_new
        }

        /// Waits up to `timeout` for the first message, if none are currently available, if some are available (and `wait_for_new` is false) it returns immediately
        pub fn process_queue_timeout(
            &mut self,
            timeout: Duration,
            wait_for_new: bool,
            mut cb: impl FnMut(&T),
        ) {
            let has_new = self.process_queue_ready(&mut cb);
            if !has_new && (wait_for_new || self.queue.lock().is_empty()) {
                match self.recv.recv_timeout(timeout) {
                    Ok(item) => {
                        cb(&item);
                        self.queue.lock().enqueue(item);
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        thread::sleep(timeout);
                    }
                }
            }
        }

        /// iterator over the currently available messages in priority order, any items not iterated when the iterator is dropped are left
        pub fn drain(&mut self) -> super::Drain<T, MutexGuard<'_, PriorityQueue<T>>> {
            PriorityQueue::drain_deref(self.queue.lock())
        }

        pub fn enqueue(&mut self, item: T) {
            self.queue.lock().enqueue(item);
        }

        pub fn queue(&self) -> Arc<Mutex<PriorityQueue<T>>> {
            self.queue.clone()
        }
    }

    /// Produces an mpsc channel where, in the event that multiple jobs are already ready, they are produced in priority order
    pub(crate) fn channel<T: Prioritised>(
        merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
    ) -> (crossbeam_channel::Sender<T>, Receiver<T>) {
        let (send, recv) = crossbeam_channel::unbounded();
        (
            send,
            Receiver {
                queue: Arc::new(Mutex::new(PriorityQueue::new(merge_fn))),
                recv,
            },
        )
    }

    #[cfg(test)]
    mod test {
        use std::time::{Duration, Instant};

        use super::*;

        #[derive(Debug, PartialEq, Eq)]
        struct Tester(u8);

        impl Prioritised for Tester {
            type Priority = u8;

            fn priority(&self) -> Self::Priority {
                self.0.into()
            }
        }

        #[test]
        fn timeout_expires() {
            let (_send, mut recv) = channel::<Tester>(None);
            recv.process_queue_timeout(Duration::from_micros(1), false, |_| {});
            assert_eq!(recv.drain().count(), 0);
        }

        #[test]
        fn returns_immediately() {
            let (send, mut recv) = channel::<Tester>(None);
            send.send(Tester(0)).unwrap();
            let instant = Instant::now();
            recv.process_queue_timeout(Duration::from_millis(1), false, |_| {});
            assert_eq!(recv.drain().next().unwrap(), Tester(0));
            assert!(Instant::now().duration_since(instant) < Duration::from_millis(1));
        }

        #[test]
        fn bunch_of_items_are_prioritised() {
            let (send, mut recv) = channel::<Tester>(None);
            send.send(Tester(2)).unwrap();
            send.send(Tester(3)).unwrap();
            send.send(Tester(1)).unwrap();
            recv.process_queue_timeout(Duration::from_millis(1), false, |_| {});
            let items: Vec<_> = recv.drain().collect();
            assert_eq!(items, vec![Tester(3), Tester(2), Tester(1)]);
        }
    }
}
