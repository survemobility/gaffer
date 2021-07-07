use std::{
    fmt,
    iter::Iterator,
    time::{Duration, Instant},
};

use crate::Prioritised;

use self::util::{may_be_taken::SkipIterator, prioritized_mpsc, Drain};

pub(crate) mod util;

/// Contains a prioritised queue of jobs, adding recurring jobs which should always be scheduled with some interval
pub struct SourceManager<J: Prioritised, R = IntervalRecurringJob<J>> {
    queue: prioritized_mpsc::Receiver<J>,
    recurring: Vec<R>,
}

impl<J: Prioritised + Send + Clone + 'static> SourceManager<J, IntervalRecurringJob<J>> {
    /// Set a job as recurring, the job will be enqueued every time `interval` passes since the last enqueue of a matching job
    pub fn set_recurring(&mut self, interval: Duration, last_enqueue: Instant, job: J) {
        self.recurring.push(IntervalRecurringJob {
            interval,
            last_enqueue,
            job,
        });
    }
}

impl<J: Prioritised, R> fmt::Debug for SourceManager<J, R>
where
    <J as Prioritised>::Priority: fmt::Debug,
    J: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.queue.fmt(f)
    }
}

impl<J: Prioritised + Send + 'static, R: RecurringJob<J>> SourceManager<J, R> {
    pub fn new() -> (crossbeam_channel::Sender<J>, SourceManager<J, R>) {
        let (send, recv) = prioritized_mpsc::channel();
        (
            send,
            SourceManager {
                queue: recv,
                recurring: vec![],
            },
        )
    }

    pub fn new_with_recurring(
        recurring: Vec<R>,
    ) -> (crossbeam_channel::Sender<J>, SourceManager<J, R>) {
        let (send, recv) = prioritized_mpsc::channel();
        (
            send,
            SourceManager {
                queue: recv,
                recurring,
            },
        )
    }

    /// Get the next batch of prioritised jobs
    ///
    /// Maximum wait duration would be the longest interval of all of the recurring jobs, or an arbitrary timeout. It could return immediately. It could return with no jobs. The caller should only iterate as many jobs as it can execute, the iterator should be dropped without iterating the rest.
    ///
    /// wait_for_new: if set, only returns immedaitely if there are new jobs inthe queue
    pub fn get(&mut self, wait_for_new: bool) -> Drain<'_, J> {
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
        self.queue.drain()
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

pub trait RecurringJob<J> {
    fn get(&self) -> Option<J>;
    fn job_enqueued(&mut self, job: &J);
    fn max_sleep(&self) -> Instant;
}

pub struct IntervalRecurringJob<J> {
    pub(crate) last_enqueue: Instant,
    pub(crate) interval: Duration,
    pub(crate) job: J,
}

impl<J: Prioritised + Clone> RecurringJob<J> for IntervalRecurringJob<J> {
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

pub enum NeverRecur {}

impl<J> RecurringJob<J> for NeverRecur {
    fn get(&self) -> Option<J> {
        panic!()
    }

    fn job_enqueued(&mut self, _job: &J) {
        panic!()
    }

    fn max_sleep(&self) -> Instant {
        panic!()
    }
}

pub struct Iter<'m, J: Prioritised> {
    queue: Option<Drain<'m, J>>,
}

impl<'m, J: Prioritised> Iterator for Iter<'m, J> {
    type Item = J;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(queue) = &mut self.queue {
            queue.next()
        } else {
            None
        }
    }
}

impl<'m, J: Prioritised> SkipIterator for Iter<'m, J> {
    fn peek_next(&mut self) -> Option<&Self::Item> {
        if let Some(queue) = &mut self.queue {
            queue.peek_next()
        } else {
            None
        }
    }

    fn skip_next(&mut self) {
        if let Some(queue) = &mut self.queue {
            queue.skip_next();
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    use crate::Prioritised;

    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone)]
    struct Tester(u8);

    impl Prioritised for Tester {
        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0
        }

        fn matches(&self, job: &Self) -> bool {
            self.0 == job.0
        }
    }

    #[test]
    fn priority_queue() {
        let (send, mut manager) = SourceManager::<_, NeverRecur>::new();
        send.send(Tester(2)).unwrap();
        send.send(Tester(3)).unwrap();
        send.send(Tester(1)).unwrap();
        assert_eq!(
            manager.get(false).collect::<Vec<_>>(),
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
            manager.get(false).collect::<Vec<_>>(),
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
            manager.get(false).collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        let before = Instant::now();
        assert_eq!(
            manager.get(false).collect::<Vec<_>>(),
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
            manager.get(false).take(1).collect::<Vec<_>>(),
            vec![Tester(3)]
        );
        assert_eq!(
            manager.get(false).collect::<Vec<_>>(),
            vec![Tester(2), Tester(1)]
        );
    }

    #[test]
    fn queued_resets_recurring() {
        let (send, mut manager) = SourceManager::new();
        let half_interval_ago = Instant::now() - Duration::from_micros(500);
        manager.set_recurring(Duration::from_millis(1), half_interval_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), half_interval_ago, Tester(2));
        manager.set_recurring(Duration::from_millis(1), half_interval_ago, Tester(3));
        send.send(Tester(2)).unwrap();
        assert_eq!(manager.get(false).collect::<Vec<_>>(), vec![Tester(2)]);
        assert_eq!(
            manager.get(false).collect::<Vec<_>>(),
            vec![Tester(3), Tester(1)]
        );
        assert_eq!(manager.get(false).collect::<Vec<_>>(), vec![Tester(2)]);
    }

    #[test]
    fn queue_received_during_poll_wait() {
        let (send, mut manager) = SourceManager::new();
        let now = Instant::now();
        manager.set_recurring(Duration::from_millis(1), now, Tester(1));
        manager.set_recurring(Duration::from_millis(1), now, Tester(2));
        manager.set_recurring(Duration::from_millis(1), now, Tester(3));
        thread::spawn(move || {
            thread::sleep(Duration::from_micros(10));
            send.send(Tester(2)).unwrap()
        });
        assert_eq!(manager.get(false).collect::<Vec<_>>(), vec![Tester(2)]);
    }

    #[test]
    fn priority_order_queue_and_recurring() {
        let (send, mut manager) = SourceManager::new();
        let one_min_ago = Instant::now() - Duration::from_secs(60);
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(1));
        manager.set_recurring(Duration::from_millis(1), one_min_ago, Tester(3));
        send.send(Tester(2)).unwrap();
        assert_eq!(
            manager.get(false).collect::<Vec<_>>(),
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
            manager.get(false).collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
        assert!(Instant::now().duration_since(before) < Duration::from_micros(100));
    }
}
