use gaffer_queue::{Prioritised, PriorityQueue};
use parking_lot::MutexGuard;
use std::{borrow::BorrowMut, ops::DerefMut, thread, time::Duration};

use crate::{Job, MergeResult};

#[derive(Debug)]
pub(crate) struct PrioritisedJob<T: Job>(pub T);

impl<T: Job> Prioritised for PrioritisedJob<T> {
    type Priority = T::Priority;

    fn priority(&self) -> Self::Priority {
        self.0.priority()
    }
}

pub(crate) struct Receiver<T: Job> {
    recv: crossbeam_channel::Receiver<T>,
    merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
}

impl<T: Job> Receiver<T> {
    /// Processes things currently ready in the queue without blocking
    pub fn process_queue_ready(
        &mut self,
        queue: &mut PriorityQueue<PrioritisedJob<T>>,
        mut cb: impl FnMut(&T),
    ) -> bool {
        let mut has_new = false;
        for item in self.recv.try_iter() {
            cb(&item);
            self.enqueue_locked(queue, item);
            has_new = true;
        }
        has_new
    }

    /// Waits up to `timeout` for the first message, if none are currently available, if some are available (and `wait_for_new` is false) it returns immediately
    pub fn process_queue_timeout<'a, Q: BorrowMut<PriorityQueue<PrioritisedJob<T>>>>(
        &mut self,
        queue: &mut MutexGuard<'a, Q>,
        timeout: Duration,
        wait_for_new: bool,
        mut cb: impl FnMut(&T),
    ) {
        let has_new = self.process_queue_ready(queue.deref_mut().borrow_mut(), &mut cb);
        if !has_new && (wait_for_new || queue.deref_mut().borrow_mut().is_empty()) {
            let recv_result = MutexGuard::unlocked(queue, || self.recv.recv_timeout(timeout));
            match recv_result {
                Ok(item) => {
                    cb(&item);
                    queue.deref_mut().borrow_mut().enqueue(PrioritisedJob(item));
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    MutexGuard::unlocked(queue, || thread::sleep(timeout));
                }
            }
        }
    }

    fn enqueue_locked(&self, queue: &mut PriorityQueue<PrioritisedJob<T>>, mut job: T) {
        let priority = job.priority();
        if let Some(merge_fn) = self.merge_fn {
            for existing in queue.iter_mut() {
                match (merge_fn)(job, &mut existing.0) {
                    MergeResult::NotMerged(the_item) => job = the_item,
                    MergeResult::Success => {
                        if existing.priority() != priority {
                            todo!("priority change needs to be handled by the queue, maybe better if priority is stored outside the item");
                            // let job = bucket.remove(idx).unwrap();
                            // self.enqueue(job);
                        }
                        return;
                    }
                }
            }
        }
        queue.enqueue(PrioritisedJob(job));
    }
}

pub(crate) fn channel<T: Job>(
    merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
) -> (crossbeam_channel::Sender<T>, Receiver<T>) {
    let (send, recv) = crossbeam_channel::unbounded();
    (send, Receiver { recv, merge_fn })
}

#[cfg(test)]
mod test {
    use std::time::{Duration, Instant};

    use parking_lot::Mutex;

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Tester(u8);

    impl Job for Tester {
        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0.into()
        }

        type Exclusion = ();

        fn exclusion(&self) -> Self::Exclusion {
            todo!()
        }

        fn execute(self) {
            todo!()
        }
    }

    #[test]
    fn timeout_expires() {
        let (_send, mut recv) = channel::<Tester>(None);
        let queue = Mutex::new(PriorityQueue::new());
        recv.process_queue_timeout(&mut queue.lock(), Duration::from_micros(1), false, |_| {});
        assert_eq!(
            PriorityQueue::drain_where(queue.lock(), |_| true).count(),
            0
        );
    }

    #[test]
    fn returns_immediately() {
        let (send, mut recv) = channel::<Tester>(None);
        send.send(Tester(0)).unwrap();
        let instant = Instant::now();
        let queue = Mutex::new(PriorityQueue::new());
        recv.process_queue_timeout(&mut queue.lock(), Duration::from_millis(1), false, |_| {});
        assert_eq!(
            PriorityQueue::drain_where(queue.lock(), |_| true)
                .next()
                .unwrap()
                .0,
            Tester(0)
        );
        assert!(Instant::now().duration_since(instant) < Duration::from_millis(1));
    }

    #[test]
    fn bunch_of_items_are_prioritised() {
        let (send, mut recv) = channel::<Tester>(None);
        send.send(Tester(2)).unwrap();
        send.send(Tester(3)).unwrap();
        send.send(Tester(1)).unwrap();
        let queue = Mutex::new(PriorityQueue::new());
        recv.process_queue_timeout(&mut queue.lock(), Duration::from_millis(1), false, |_| {});
        let items: Vec<_> = PriorityQueue::drain_where(queue.lock(), |_| true)
            .map(|t| t.0)
            .collect();
        assert_eq!(items, vec![Tester(3), Tester(2), Tester(1)]);
    }
}
