use gaffer_queue::{Prioritised, PriorityQueue};
use parking_lot::Mutex;
use std::{fmt, sync::Arc, thread, time::Duration};

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
    queue: Arc<Mutex<PriorityQueue<PrioritisedJob<T>>>>,
    recv: crossbeam_channel::Receiver<T>,
    merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
}

impl<T: Job> fmt::Debug for Receiver<T>
where
    <T as Job>::Priority: fmt::Debug,
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.queue.fmt(f)
    }
}

impl<T: Job> Receiver<T> {
    /// Processes things currently ready in the queue without blocking
    pub fn process_queue_ready(&mut self, mut cb: impl FnMut(&T)) -> bool {
        let mut has_new = false;
        let mut queue = self.queue.lock();
        for item in self.recv.try_iter() {
            cb(&item);
            self.enqueue_locked(&mut queue, item);
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
                    self.queue.lock().enqueue(PrioritisedJob(item));
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    thread::sleep(timeout);
                }
            }
        }
    }

    /// iterator over the currently available messages in priority order, any items not iterated when the iterator is dropped are left
    pub fn drain_where<'s, 'p: 's, P: FnMut(&PrioritisedJob<T>) -> bool + 'p>(
        &'s mut self,
        predicate: P,
    ) -> impl Iterator<Item = T> + 's {
        PriorityQueue::drain_where_deref(self.queue.lock(), predicate)
            .map(|PrioritisedJob(job)| job) // would be nice to map the predicate as well to remove the PrioritisedJob wrapper, but I'm struggling composing the functions
    }

    pub fn enqueue(&self, item: T) {
        Self::enqueue_locked(self, &mut self.queue.lock(), item);
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

    pub fn queue(&self) -> Arc<Mutex<PriorityQueue<PrioritisedJob<T>>>> {
        self.queue.clone()
    }
}

/// Produces an mpsc channel where, in the event that multiple jobs are already ready, they are produced in priority order
pub(crate) fn channel<T: Job>(
    merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
) -> (crossbeam_channel::Sender<T>, Receiver<T>) {
    let (send, recv) = crossbeam_channel::unbounded();
    (
        send,
        Receiver {
            queue: Arc::new(Mutex::new(PriorityQueue::new())),
            recv,
            merge_fn,
        },
    )
}

#[cfg(test)]
mod test {
    use std::time::{Duration, Instant};

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
        recv.process_queue_timeout(Duration::from_micros(1), false, |_| {});
        assert_eq!(recv.drain_where(|_| true).count(), 0);
    }

    #[test]
    fn returns_immediately() {
        let (send, mut recv) = channel::<Tester>(None);
        send.send(Tester(0)).unwrap();
        let instant = Instant::now();
        recv.process_queue_timeout(Duration::from_millis(1), false, |_| {});
        assert_eq!(recv.drain_where(|_| true).next().unwrap(), Tester(0));
        assert!(Instant::now().duration_since(instant) < Duration::from_millis(1));
    }

    #[test]
    fn bunch_of_items_are_prioritised() {
        let (send, mut recv) = channel::<Tester>(None);
        send.send(Tester(2)).unwrap();
        send.send(Tester(3)).unwrap();
        send.send(Tester(1)).unwrap();
        recv.process_queue_timeout(Duration::from_millis(1), false, |_| {});
        let items: Vec<_> = recv.drain_where(|_| true).collect();
        assert_eq!(items, vec![Tester(3), Tester(2), Tester(1)]);
    }
}
