//! Handles job sources

use gaffer_queue::{Prioritised, PriorityQueue};
use parking_lot::MutexGuard;
use std::{
    borrow::BorrowMut,
    iter::Iterator,
    ops::{Deref, DerefMut},
    thread,
    time::{Duration, Instant},
};

use crate::{runner::Supervisor, Job, MergeResult};

/// Manages sources of jobs for a runner, including:
/// * reading jobs from a channel and waiting on the channel
/// * scheduling recurring jobs after timeouts have passed
/// * merging jobs into the `PriorityQueue`
pub(crate) struct SourceManager<J: Job, R> {
    recurring: Vec<R>,
    receiver: Receiver<J>,
}

#[cfg(test)]
impl<J: Job + Send + RecurrableJob + 'static> SourceManager<J, IntervalRecurringJob<J>> {
    /// Set a job as recurring, the job will be enqueued every time `interval` passes since the last enqueue of a matching job
    fn set_recurring(&mut self, interval: Duration, last_enqueue: Instant, job: J) {
        self.recurring.push(IntervalRecurringJob {
            last_enqueue,
            interval,
            job,
        });
    }
}

impl<J, R> SourceManager<J, R>
where
    J: Job + Send + 'static,
    R: RecurringJob<Job = J>,
{
    /// Create a new `(Sender, SourceManager<>)` pair with the provided recurring jobs
    pub fn new(
        recurring: Vec<R>,
        merge_fn: Option<fn(J, &mut J) -> MergeResult<J>>,
    ) -> (crossbeam_channel::Sender<J>, Self) {
        let (send, receiver) = channel::<J>(merge_fn);
        (
            send,
            Self {
                recurring,
                receiver,
            },
        )
    }

    /// get the timeout to wait for the queue based on the status of the recurring jobs
    fn queue_timeout(&mut self) -> Duration {
        if let Some(poll_time) = self.soonest_recurring() {
            poll_time
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO) // a recurring job is ready
        } else {
            Duration::from_secs(5) // there are no pollers so this is kinda abitrary
        }
    }

    /// The soonest instant when a recurring job would need to be created
    fn soonest_recurring(&self) -> Option<Instant> {
        self.recurring.iter().map(R::max_sleep).min()
    }
}

impl<J: Job, R: RecurringJob<Job = J> + Send + 'static>
    gaffer_runner::Loader<crate::runner::Task<J>> for SourceManager<J, R>
{
    type Scheduler = Supervisor<J>;

    /// Get the next batch of prioritised jobs
    ///
    /// Maximum wait duration would be the longest interval of all of the recurring jobs, or an arbitrary timeout. It could return immediately. It could return with no jobs. The caller should only iterate as many jobs as it can execute, the iterator should be dropped without iterating the rest.
    ///
    /// wait_for_new: if set, only returns immedaitely if there are new jobs inthe queue
    fn load(&mut self, wait_for_new: bool, mut scheduler: MutexGuard<'_, Self::Scheduler>) {
        let timeout = self.queue_timeout();
        let recurring = &mut self.recurring;
        if timeout == Duration::ZERO {
            self.receiver
                .process_queue_ready(scheduler.deref_mut().borrow_mut(), |new_enqueue| {
                    for recurring in recurring.iter_mut() {
                        recurring.job_enqueued(new_enqueue);
                    }
                });
        } else {
            self.receiver.process_queue_timeout(
                &mut scheduler,
                timeout,
                wait_for_new,
                |new_enqueue| {
                    for recurring in recurring.iter_mut() {
                        recurring.job_enqueued(new_enqueue);
                    }
                },
            );
        }
        for item in self.recurring.iter().flat_map(R::get).collect::<Vec<_>>() {
            for recurring in &mut self.recurring {
                recurring.job_enqueued(&item);
            }
            scheduler.enqueue(item);
        }
    }
}

/// Defines how a job recurs
pub trait RecurringJob {
    type Job;

    /// Get the job if it is ready to recur
    fn get(&self) -> Option<Self::Job>;
    /// Notifies the recurring job about any job that has ben enqueued so that it can push back it's next occurance
    fn job_enqueued(&mut self, job: &Self::Job);
    /// Returns the latest `Instant` that the caller could sleep until before it should call `get()` again
    fn max_sleep(&self) -> Instant;
}

impl<J> RecurringJob for Box<dyn RecurringJob<Job = J> + Send> {
    type Job = J;

    fn get(&self) -> Option<J> {
        self.deref().get()
    }

    fn job_enqueued(&mut self, job: &J) {
        self.deref_mut().job_enqueued(job)
    }

    fn max_sleep(&self) -> Instant {
        self.deref().max_sleep()
    }
}

/// A job which can be rescheduled through cloning
pub trait RecurrableJob: Clone {
    /// When a job matching a `Recurrablejob` is scheduled, this resets the recurrance interval
    fn matches(&self, other: &Self) -> bool;
}

/// Recurring job which works by recording the last time a job was enqueued and reenqueueing after some interval
pub struct IntervalRecurringJob<J: RecurrableJob> {
    pub(crate) last_enqueue: Instant,
    pub(crate) interval: Duration,
    pub(crate) job: J,
}

impl<J: RecurrableJob> RecurringJob for IntervalRecurringJob<J> {
    type Job = J;

    fn get(&self) -> Option<J> {
        if Instant::now() > self.last_enqueue + self.interval {
            Some(self.job.clone())
        } else {
            None
        }
    }

    fn job_enqueued(&mut self, job: &J) {
        if self.job.matches(job) {
            self.last_enqueue = Instant::now();
        }
    }

    fn max_sleep(&self) -> Instant {
        self.last_enqueue + self.interval
    }
}

/// Just until the never type is stable, this represents that the job does not recur
enum NeverRecur {}

impl<J> RecurringJob for (NeverRecur, J) {
    type Job = J;

    fn get(&self) -> Option<J> {
        unreachable!()
    }

    fn job_enqueued(&mut self, _job: &J) {
        unreachable!()
    }

    fn max_sleep(&self) -> Instant {
        unreachable!()
    }
}

#[derive(Debug)]
pub(crate) struct PrioritisedJob<T: Job>(pub T);

impl<T: Job> Prioritised for PrioritisedJob<T> {
    type Priority = T::Priority;

    fn priority(&self) -> Self::Priority {
        self.0.priority()
    }
}

struct Receiver<T: Job> {
    recv: crossbeam_channel::Receiver<T>,
    merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
}

impl<T: Job> Receiver<T> {
    /// Processes things currently ready in the queue without blocking
    fn process_queue_ready(
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
    fn process_queue_timeout<'a, Q: BorrowMut<PriorityQueue<PrioritisedJob<T>>>>(
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

fn channel<T: Job>(
    merge_fn: Option<fn(T, &mut T) -> MergeResult<T>>,
) -> (crossbeam_channel::Sender<T>, Receiver<T>) {
    let (send, recv) = crossbeam_channel::unbounded();
    (send, Receiver { recv, merge_fn })
}

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    use gaffer_queue::PriorityQueue;
    use gaffer_runner::{Loader, Scheduler};
    use parking_lot::Mutex;

    use crate::runner::Task;

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Tester(u8);

    impl Job for Tester {
        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0
        }

        type Exclusion = ();

        fn exclusion(&self) -> Self::Exclusion {}

        fn execute(self) {
            todo!()
        }
    }

    impl RecurrableJob for Tester {
        fn matches(&self, other: &Self) -> bool {
            self.eq(other)
        }
    }

    #[test]
    fn priority_queue() {
        let mut queue = PriorityQueue::new();
        let (send, mut recv) = channel(None);
        send.send(Tester(2)).unwrap();
        send.send(Tester(3)).unwrap();
        send.send(Tester(1)).unwrap();
        recv.process_queue_ready(&mut queue, |_| ());
        assert_eq!(
            PriorityQueue::drain_where(&mut queue, |_| true)
                .map(|t| t.0)
                .collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        )
    }

    #[test]
    fn recurring_ready() {
        let scheduler = Mutex::new(Supervisor::new());
        let (_send, mut manager) = SourceManager::new(vec![], None);
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(2));
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(3));
        let before = Instant::now();
        manager.load(false, scheduler.lock()); // need to do this on the other tests to make them work,
        assert_eq!(
            scheduler
                .into_inner()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        assert!(Instant::now().duration_since(before) < Duration::from_millis(1));
    }

    #[test]
    fn recurring_interval() {
        let scheduler = Mutex::new(Supervisor::new());
        let (_send, mut manager) = SourceManager::new(vec![], None);
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(2));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(3));
        manager.load(false, scheduler.lock()); // need to do this on the other tests to make them work,
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        let before = Instant::now();
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        assert!(
            Instant::now().duration_since(before) > Duration::from_millis(1),
            "duration only : {:?}",
            Instant::now().duration_since(before)
        );
    }

    #[test]
    fn recurring_not_duplicated() {
        let scheduler = Mutex::new(Supervisor::new());
        let (_send, mut manager) = SourceManager::new(vec![], None);
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(2));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(3));
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 1)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(3)]
        );
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(2), Tester(1)]
        );
    }

    #[test]
    fn queued_resets_recurring() {
        let scheduler = Mutex::new(Supervisor::new());
        let (send, mut manager) = SourceManager::new(vec![], None);
        let start = Instant::now();
        let half_interval_ago = start - Duration::from_millis(5);
        manager.set_recurring(Duration::from_millis(10), half_interval_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(10), half_interval_ago, Tester(2));
        manager.set_recurring(Duration::from_millis(10), half_interval_ago, Tester(3));
        send.send(Tester(2)).unwrap();
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(2)],
            "Wrong result after {:?}",
            Instant::now().duration_since(start)
        );
        let restart = Instant::now();
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(3), Tester(1)],
            "Wrong result after {:?}",
            Instant::now().duration_since(restart)
        );
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(2)]
        );
    }

    #[test]
    fn queue_received_during_poll_wait() {
        let scheduler = Mutex::new(Supervisor::new());
        let (send, mut manager) = SourceManager::new(vec![], None);
        let now = Instant::now();
        manager.set_recurring(Duration::from_millis(1), now, Tester(1));
        manager.set_recurring(Duration::from_millis(1), now, Tester(3));
        send.send(Tester(2)).unwrap();
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(2)],
            "Wrong result after {:?}",
            Instant::now().duration_since(now)
        );
    }

    #[test]
    fn priority_order_queue_and_recurring() {
        let scheduler = Mutex::new(Supervisor::new());
        let (send, mut manager) = SourceManager::new(vec![], None);
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(3));
        send.send(Tester(2)).unwrap();
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
    }

    #[test]
    fn queue_not_awaited_with_ready_recurring() {
        let scheduler = Mutex::new(Supervisor::new());
        let (send, mut manager) = SourceManager::new(vec![], None);
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(2));
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(3));
        let b1 = Arc::new(Barrier::new(2));
        let b2 = b1.clone();
        thread::spawn(move || {
            b1.wait();
            thread::sleep(Duration::from_millis(5));
            send.send(Tester(2)).unwrap()
        });
        b2.wait();
        let before = Instant::now();
        manager.load(false, scheduler.lock());
        assert_eq!(
            scheduler
                .lock()
                .steal(&[None, None, None], 3)
                .into_iter()
                .map(|Task(t)| t)
                .collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        assert!(Instant::now().duration_since(before) < Duration::from_millis(1));
    }
}

#[cfg(test)]
mod test2 {
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
