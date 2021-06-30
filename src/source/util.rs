use std::{
    cmp::Reverse,
    collections::{BTreeMap, VecDeque},
};

use crate::{MergeResult, Prioritised};

pub(crate) struct PriorityQueue<T: Prioritised> {
    map: BTreeMap<Reverse<T::Priority>, VecDeque<T>>,
}

impl<T: Prioritised> PriorityQueue<T> {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// Enqueues the item so that it will be iterated before any existing items in the queue with a lower priority and after any existing items with the same or higher priority.
    /// If `T` has a merge function in `T::ATTEMPT_MERGE_INTO`, the item will be merged into the highest priority existing item which merges successfully. The queue should maintain a state where everything that can be merged is merged, as long as the merge function is transitive in it's successes.
    pub fn enqueue(&mut self, mut item: T) {
        if let Some(attempt_merge) = T::ATTEMPT_MERGE_INTO {
            for (Reverse(priority), bucket) in &mut self.map {
                // for now we iterate ove the whole queue to look for merges, not the best solution
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

    pub fn enqueue_internal(&mut self, item: T) {
        self.map
            .entry(Reverse(item.priority()))
            .or_default()
            .push_back(item);
    }

    pub fn dequeue(&mut self) -> Option<T> {
        for queue in self.map.values_mut() {
            if let Some(next) = queue.pop_front() {
                return Some(next);
            }
            // we could have an `else` clause here to remove the empty sub-queue, but it's expected that a few priority levels will be used and so it's better to leave the m and avoid the allocations
        }
        None
    }

    /// drains each element iterated, once the iterator is dropped, *unlike `drain` implementations in the standard library, any remaining items are left in the queue
    pub fn drain(&mut self) -> Drain<'_, T> {
        Drain { queue: self }
    }

    pub fn is_empty(&self) -> bool {
        self.map.iter().all(|(_, queue)| queue.is_empty())
    }

    pub fn first(&self) -> Option<&T> {
        for queue in self.map.values() {
            if let Some(next) = queue.front() {
                return Some(next);
            }
            // we could have an `else` clause here to remove the empty sub-queue, but it's expected that a few priority levels will be used and so it's better to leave the m and avoid the allocations
        }
        None
    }
}

pub(crate) struct Drain<'q, T: Prioritised> {
    queue: &'q mut PriorityQueue<T>,
}

impl<'q, T: Prioritised> Drain<'q, T> {
    pub fn peek(&self) -> Option<&T> {
        self.queue.first()
    }
}

impl<'q, T: Prioritised> Iterator for Drain<'q, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.dequeue()
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
    fn drain_peek() {
        let mut queue = PriorityQueue::new();
        queue.enqueue(PrioritisedJob(1, 'a'));
        queue.enqueue(PrioritisedJob(1, 'b'));
        let mut drain = queue.drain();
        assert_eq!(drain.peek(), Some(&PrioritisedJob(1, 'a')));
        let vals: String = (&mut drain).take(1).map(|j| j.1).collect();
        assert_eq!(vals, "a");
        assert_eq!(drain.peek(), Some(&PrioritisedJob(1, 'b')));
        let vals: String = drain.map(|j| j.1).collect();
        assert_eq!(vals, "b");
    }

    #[derive(PartialEq, Eq, Debug)]
    struct MergableJob(u8, char);

    impl Prioritised for MergableJob {
        type Priority = TestPriority;

        fn priority(&self) -> Self::Priority {
            TestPriority(self.0)
        }

        const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> =
            Some(|me: Self, other: &mut Self| -> MergeResult<Self> {
                if me.1 == other.1 {
                    other.0 = other.0.max(me.0);
                    MergeResult::Success
                } else {
                    MergeResult::NotMerged(me)
                }
            });
    }

    #[test]
    fn priority_queue_elements_are_merged() {
        let mut queue = PriorityQueue::new();
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
    use std::{
        sync::mpsc::{self, RecvTimeoutError},
        time::Duration,
    };

    use crate::Prioritised;

    use super::PriorityQueue;

    pub struct Receiver<T: Prioritised> {
        queue: PriorityQueue<T>,
        recv: mpsc::Receiver<T>,
    }

    impl<T: Prioritised> Receiver<T> {
        /// Waits up to `timeout` for the first message, if none are currently available, if some are available it returns immediately with an iterator over the currently available messages in priority order, any items not iterated when the iterator is dropped are left
        pub(crate) fn iter_timeout(
            &mut self,
            timeout: Duration,
        ) -> Result<super::Drain<T>, RecvTimeoutError> {
            for item in self.recv.try_iter() {
                self.queue.enqueue(item);
            }
            if !self.queue.is_empty() {
                Ok(self.queue.drain())
            } else {
                self.recv.recv_timeout(timeout).map(move |message| {
                    self.queue.enqueue(message);
                    self.queue.drain()
                })
            }
        }
    }

    /// Produces an mpsc channel where, in the event that multiple jobs are already ready, they are produced in priority order
    pub fn channel<T: Prioritised>() -> (mpsc::Sender<T>, Receiver<T>) {
        let (send, recv) = mpsc::channel();
        (
            send,
            Receiver {
                queue: PriorityQueue::new(),
                recv,
            },
        )
    }

    #[cfg(test)]
    mod test {
        use std::{
            thread,
            time::{Duration, Instant},
        };

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
        fn iter_timeout_expires() {
            let (_send, mut recv) = channel::<Tester>();
            assert_eq!(
                recv.iter_timeout(Duration::from_micros(1)).err().unwrap(),
                RecvTimeoutError::Timeout
            );
        }

        #[test]
        fn iter_timeout_returns_immediately() {
            let (send, mut recv) = channel::<Tester>();
            thread::spawn(move || {
                send.send(Tester(0)).unwrap();
            });
            let instant = Instant::now();
            assert_eq!(
                recv.iter_timeout(Duration::from_millis(1))
                    .unwrap()
                    .next()
                    .unwrap(),
                Tester(0)
            );
            assert!(Instant::now().duration_since(instant) < Duration::from_millis(1));
        }

        #[test]
        fn iter_timeout_bunch_of_items_are_prioritised() {
            let (send, mut recv) = channel::<Tester>();
            send.send(Tester(2)).unwrap();
            send.send(Tester(3)).unwrap();
            send.send(Tester(1)).unwrap();
            let items: Vec<_> = recv
                .iter_timeout(Duration::from_millis(1))
                .unwrap()
                .collect();
            assert_eq!(items, vec![Tester(3), Tester(2), Tester(1)]);
        }
    }
}
