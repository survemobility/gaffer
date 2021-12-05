//! Handles job sources

use gaffer_queue::PriorityQueue;
use parking_lot::Mutex;
use std::{
    fmt,
    iter::Iterator,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{Job, MergeResult};

use self::prioritized_mpsc::PrioritisedJob;

pub(crate) mod prioritized_mpsc;

/// Contains a prioritised queue of jobs, adding recurring jobs which should always be scheduled with some interval
pub(crate) struct SourceManager<J: Job, R> {
    queue: prioritized_mpsc::Receiver<J>,
    recurring: Vec<R>,
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

impl<J: Job, R> fmt::Debug for SourceManager<J, R>
where
    <J as Job>::Priority: fmt::Debug,
    J: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.queue.fmt(f)
    }
}

impl<J: Job + Send + 'static, R: RecurringJob<J>> SourceManager<J, R> {
    #[cfg(test)]
    /// Create a new `(Sender, SourceManager<>)` pair
    pub fn new() -> (crossbeam_channel::Sender<J>, SourceManager<J, R>) {
        let (send, recv) = prioritized_mpsc::channel(None);
        (
            send,
            SourceManager {
                queue: recv,
                recurring: vec![],
            },
        )
    }

    /// Create a new `(Sender, SourceManager<>)` pair with the provided recurring jobs
    pub fn new_with_recurring(
        recurring: Vec<R>,
        merge_fn: Option<fn(J, &mut J) -> MergeResult<J>>,
    ) -> (crossbeam_channel::Sender<J>, SourceManager<J, R>) {
        let (send, recv) = prioritized_mpsc::channel(merge_fn);
        (
            send,
            SourceManager {
                queue: recv,
                recurring,
            },
        )
    }

    #[cfg(test)]
    pub fn get<'s, 'p: 's, P: for<'j> FnMut(&'j PrioritisedJob<J>) -> bool + 'p>(
        &'s mut self,
        wait_for_new: bool,
        predicate: P,
    ) -> impl Iterator<Item = J> + 's {
        self.process(wait_for_new);
        self.queue.drain_where(predicate)
    }

    /// Get the next batch of prioritised jobs
    ///
    /// Maximum wait duration would be the longest interval of all of the recurring jobs, or an arbitrary timeout. It could return immediately. It could return with no jobs. The caller should only iterate as many jobs as it can execute, the iterator should be dropped without iterating the rest.
    ///
    /// wait_for_new: if set, only returns immedaitely if there are new jobs inthe queue
    pub fn process(&mut self, wait_for_new: bool) {
        let timeout = self.queue_timeout();
        let recurring = &mut self.recurring;
        if timeout == Duration::ZERO {
            self.queue.process_queue_ready(|new_enqueue| {
                for recurring in recurring.iter_mut() {
                    recurring.job_enqueued(new_enqueue);
                }
            });
        } else {
            self.queue
                .process_queue_timeout(timeout, wait_for_new, |new_enqueue| {
                    for recurring in recurring.iter_mut() {
                        recurring.job_enqueued(new_enqueue);
                    }
                });
        }
        for item in self.recurring.iter().flat_map(R::get).collect::<Vec<_>>() {
            for recurring in &mut self.recurring {
                recurring.job_enqueued(&item);
            }
            self.queue.enqueue(item);
        }
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

    /// Gets access to the priority queue that this source uses, be careful with this `Mutex` as `get()` will also lock it.
    pub fn queue(&self) -> Arc<Mutex<PriorityQueue<PrioritisedJob<J>>>> {
        self.queue.queue()
    }
}

impl<J: Job, R: RecurringJob<J> + Send + 'static> gaffer_runner::Loader<crate::runner::Task<J>>
    for SourceManager<J, R>
{
    fn load(
        &mut self,
        idle: bool,
        scheduler: &Mutex<dyn gaffer_runner::Scheduler<crate::runner::Task<J>>>,
    ) {
        self.process(idle)
    }
}

/// Defines how a job recurs
pub trait RecurringJob<J> {
    /// Get the job if it is ready to recur
    fn get(&self) -> Option<J>;
    /// Notifies the recurring job about any job that has ben enqueued so that it can push back it's next occurance
    fn job_enqueued(&mut self, job: &J);
    /// Returns the latest `Instant` that the caller could sleep until before it should call `get()` again
    fn max_sleep(&self) -> Instant;
}

impl<J> RecurringJob<J> for Box<dyn RecurringJob<J> + Send> {
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

/// Recurring job which works by updating the last time a job was enqueued reenqueueing after some interval
pub struct IntervalRecurringJob<J: RecurrableJob> {
    pub(crate) last_enqueue: Instant,
    pub(crate) interval: Duration,
    pub(crate) job: J,
}

impl<J: RecurrableJob> RecurringJob<J> for IntervalRecurringJob<J> {
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

impl<J> RecurringJob<J> for NeverRecur {
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

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Tester(u8);

    impl Job for Tester {
        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0
        }

        type Exclusion = ();

        fn exclusion(&self) -> Self::Exclusion {
            todo!()
        }

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
        let (send, mut manager) = SourceManager::<_, NeverRecur>::new();
        send.send(Tester(2)).unwrap();
        send.send(Tester(3)).unwrap();
        send.send(Tester(1)).unwrap();
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        )
    }

    #[test]
    fn recurring_ready() {
        let (_send, mut manager) = SourceManager::new();
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(2));
        manager.set_recurring(Duration::from_secs(1), one_min_ago, Tester(3));
        let before = Instant::now();
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        assert!(Instant::now().duration_since(before) < Duration::from_millis(1));
    }

    #[test]
    fn recurring_interval() {
        let (_send, mut manager) = SourceManager::new();
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(2));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(3));
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        let before = Instant::now();
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
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
        let (_send, mut manager) = SourceManager::new();
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(2));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(3));
        assert_eq!(
            manager.get(false, |_| true).take(1).collect::<Vec<_>>(),
            vec![Tester(3)]
        );
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(2), Tester(1)]
        );
    }

    #[test]
    fn queued_resets_recurring() {
        let (send, mut manager) = SourceManager::new();
        let start = Instant::now();
        let half_interval_ago = start - Duration::from_millis(5);
        manager.set_recurring(Duration::from_millis(10), half_interval_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(10), half_interval_ago, Tester(2));
        manager.set_recurring(Duration::from_millis(10), half_interval_ago, Tester(3));
        send.send(Tester(2)).unwrap();
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(2)],
            "Wrong result after {:?}",
            Instant::now().duration_since(start)
        );
        let restart = Instant::now();
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(3), Tester(1)],
            "Wrong result after {:?}",
            Instant::now().duration_since(restart)
        );
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(2)]
        );
    }

    #[test]
    fn queue_received_during_poll_wait() {
        let (send, mut manager) = SourceManager::new();
        let now = Instant::now();
        manager.set_recurring(Duration::from_millis(1), now, Tester(1));
        manager.set_recurring(Duration::from_millis(1), now, Tester(3));
        send.send(Tester(2)).unwrap();
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(2)],
            "Wrong result after {:?}",
            Instant::now().duration_since(now)
        );
    }

    #[test]
    fn priority_order_queue_and_recurring() {
        let (send, mut manager) = SourceManager::new();
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(3));
        send.send(Tester(2)).unwrap();
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
    }

    #[test]
    fn queue_not_awaited_with_ready_recurring() {
        let (send, mut manager) = SourceManager::new();
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
        assert_eq!(
            manager.get(false, |_| true).collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        assert!(Instant::now().duration_since(before) < Duration::from_millis(1));
    }
}
