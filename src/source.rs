use std::{
    iter::Peekable,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use crate::Prioritised;

use self::util::{prioritized_mpsc, Drain};

mod util;

/// Combines waiting on a channel with polling job sources, producing available jobs in priority order and aiming to be fair between sources
pub struct SourceManager<J: Prioritised> {
    queue: prioritized_mpsc::Receiver<J>,
    sources: Vec<PollSource<J>>,
}

impl<J: Prioritised + Send + 'static> SourceManager<J> {
    pub fn new(sources: Vec<PollSource<J>>) -> (mpsc::Sender<J>, SourceManager<J>) {
        let (send, recv) = prioritized_mpsc::channel();
        (
            send,
            SourceManager {
                queue: recv,
                sources,
            },
        )
    }

    /// Get the next batch of prioritised jobs
    ///
    /// Maximum wait duration would be the longest `preferred_interval` of all of the pollers, plus the time to poll each of the pollers. It could return immediately. It could return with no jobs. The caller should only iterate as many jobs as it can execute, the iterator should be dropped without iterating the rest.
    pub fn get(&mut self) -> Iter<'_, J> {
        let timeout = self.queue_timeout();
        let queue = match self.queue.iter_timeout(timeout) {
            Ok(jobs) => Some(jobs),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                thread::sleep(timeout);
                None
            }
            Err(mpsc::RecvTimeoutError::Timeout) => None,
        };
        let mut polls = Vec::with_capacity(self.sources.len());
        for PollSource {
            pollable,
            last_poll,
        } in &mut self.sources
        {
            let interval_elapsed = Instant::now().duration_since(*last_poll);
            if interval_elapsed < pollable.min_interval {
                continue;
            }
            match (pollable.poll)() {
                Ok(jobs) => {
                    *last_poll = Instant::now();
                    polls.push(jobs.peekable());
                }
                Err(PollError::ResetInterval) => {
                    *last_poll = Instant::now();
                    break;
                }
                Err(PollError::RetryNext) => break,
            }
        }
        Iter { queue, polls }
    }

    /// get the timeout to wait for the queue based on the status of the pollers
    fn queue_timeout(&mut self) -> Duration {
        if self.poll_ready() {
            // can poll immediately, won't sleep
            Duration::ZERO
        } else if let Some(poll_time) = self.soonest_preferred_poll() {
            poll_time
                .checked_duration_since(Instant::now())
                .unwrap_or_default()
        } else {
            Duration::from_secs(10) // there are no pollers so this is kinda abitrary
        }
    }

    /// Checks whether at least one of the pollers has passed it's minimum poll interval
    fn poll_ready(&self) -> bool {
        let now = Instant::now();
        self.sources
            .iter()
            .any(|poller| poller.last_poll + poller.pollable.min_interval < now)
    }

    /// The soonest interval when a poller would prefer to be polled, or `None` if there are no pollers
    fn soonest_preferred_poll(&self) -> Option<Instant> {
        self.sources
            .iter()
            .map(|poller| poller.last_poll + poller.pollable.preferred_interval)
            .min()
    }
}

pub struct Iter<'m, J: Prioritised> {
    queue: Option<Drain<'m, J>>,
    polls: Vec<Peekable<Box<dyn Iterator<Item = J> + 'm>>>,
}

impl<'m, J: Prioritised> Iterator for Iter<'m, J> {
    type Item = J;

    fn next(&mut self) -> Option<Self::Item> {
        let highest_priority_poller = self
            .polls
            .iter_mut()
            .map(|p| (p.peek().map(|j| j.priority()), p))
            .min_by_key(|(priority, _p)| *priority);
        match (highest_priority_poller, &mut self.queue) {
            (Some((Some(priority), poller)), Some(queue)) => {
                if let Some(next_queued) = queue.peek() {
                    if next_queued.priority() > priority {
                        queue.next()
                    } else {
                        poller.next()
                    }
                } else {
                    poller.next()
                }
            }
            (Some((_priority, poller)), None) => poller.next(),
            (_, Some(queue)) => queue.next(),
            (None, None) => None,
        }
    }
}

pub struct PollSource<J: Prioritised> {
    pub pollable: PollableSource<J>,
    pub last_poll: Instant,
}

pub struct PollableSource<J> {
    pub poll: Box<dyn FnMut() -> PollOutput<J> + Send>,
    /// Minimum interval between polls, if the manager is looking for jobs before this is up, this source will be skipped
    pub min_interval: Duration,
    /// Preferred interval between polls, if the manager is idle, it will schedule a poll on or before this interval
    pub preferred_interval: Duration,
}

pub type PollOutput<J> = Result<Box<dyn Iterator<Item = J>>, PollError>;

pub enum PollError {
    /// No result was found for the poll, but the minimum poll interval should be observed before polling again
    ResetInterval,
    /// No result was found for the poll, continue for now but leave this source ready to poll
    RetryNext,
}

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, Barrier},
        time::Duration,
    };

    use crate::{Prioritised, UnrestrictedParallelism};

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Tester(u8);

    impl Prioritised for Tester {
        type Priority = UnrestrictedParallelism<u8>;

        fn priority(&self) -> Self::Priority {
            self.0.into()
        }
    }

    #[test]
    fn priority_queue() {
        let (send, recv) = util::prioritized_mpsc::channel();
        let mut manager = SourceManager {
            queue: recv,
            sources: vec![],
        };
        send.send(Tester(2)).unwrap();
        send.send(Tester(3)).unwrap();
        send.send(Tester(1)).unwrap();
        assert_eq!(
            manager.get().collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        )
    }

    #[test]
    fn poller_instant() {
        let pollable = PollableSource {
            poll: Box::new(move || Ok(Box::new(vec![Tester(1), Tester(2), Tester(3)].into_iter()))),
            min_interval: Duration::from_micros(100),
            preferred_interval: Duration::from_millis(1),
        };
        let mut manager = SourceManager {
            queue: util::prioritized_mpsc::channel().1,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_secs(60),
                pollable,
            }],
        };
        let before = Instant::now();
        assert_eq!(
            manager.get().collect::<Vec<_>>(),
            vec![Tester(1), Tester(2), Tester(3)]
        );
        assert!(Instant::now().duration_since(before) < Duration::from_micros(100));
    }

    #[test]
    fn poller_interval() {
        let pollable = PollableSource {
            poll: Box::new(move || Ok(Box::new(vec![Tester(1), Tester(2), Tester(3)].into_iter()))),
            min_interval: Duration::from_micros(100),
            preferred_interval: Duration::from_millis(1),
        };
        let mut manager = SourceManager {
            queue: util::prioritized_mpsc::channel().1,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_secs(60),
                pollable,
            }],
        };
        assert_eq!(
            manager.get().collect::<Vec<_>>(),
            vec![Tester(1), Tester(2), Tester(3)]
        );
        let before = Instant::now();
        assert_eq!(
            manager.get().collect::<Vec<_>>(),
            vec![Tester(1), Tester(2), Tester(3)]
        );
        assert!(
            Instant::now().duration_since(before) > Duration::from_millis(1),
            "duration only : {:?}",
            Instant::now().duration_since(before)
        );
    }

    #[test]
    fn queue_received_during_poll_wait() {
        let (send, recv) = util::prioritized_mpsc::channel();
        let pollable = PollableSource {
            poll: Box::new(move || Ok(Box::new(vec![Tester(1), Tester(2), Tester(3)].into_iter()))),
            min_interval: Duration::from_millis(1),
            preferred_interval: Duration::from_millis(2),
        };
        let mut manager = SourceManager {
            queue: recv,
            sources: vec![PollSource {
                last_poll: Instant::now(),
                pollable,
            }],
        };
        thread::spawn(move || {
            thread::sleep(Duration::from_micros(10));
            send.send(Tester(2)).unwrap()
        });
        assert_eq!(manager.get().collect::<Vec<_>>(), vec![Tester(2)]);
    }

    #[test]
    fn priority_order_across_sources() {
        let (send, recv) = util::prioritized_mpsc::channel();
        let pollable = PollableSource {
            poll: Box::new(move || Ok(Box::new(vec![Tester(3), Tester(2), Tester(1)].into_iter()))),
            min_interval: Duration::from_micros(100),
            preferred_interval: Duration::from_millis(1),
        };
        send.send(Tester(2)).unwrap();
        let mut manager = SourceManager {
            queue: recv,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_secs(60),
                pollable,
            }],
        };
        assert_eq!(
            manager.get().collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(2), Tester(1)]
        );
    }

    #[test]
    fn poll_ignored_in_cooloff() {
        let (send, recv) = util::prioritized_mpsc::channel();
        let pollable = PollableSource {
            poll: Box::new(move || Ok(Box::new(vec![Tester(3), Tester(2), Tester(1)].into_iter()))),
            min_interval: Duration::from_millis(1),
            preferred_interval: Duration::from_millis(100),
        };
        send.send(Tester(2)).unwrap();
        let mut manager = SourceManager {
            queue: recv,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_micros(500),
                pollable,
            }],
        };
        assert_eq!(manager.get().collect::<Vec<_>>(), vec![Tester(2)]);
    }

    #[test]
    fn queue_not_awaited_with_ready_poll() {
        let (send, recv) = util::prioritized_mpsc::channel();
        let pollable = PollableSource {
            poll: Box::new(move || Ok(Box::new(vec![Tester(3), Tester(2), Tester(1)].into_iter()))),
            min_interval: Duration::from_millis(1),
            preferred_interval: Duration::from_millis(100),
        };
        let mut manager = SourceManager {
            queue: recv,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_micros(5000),
                pollable,
            }],
        };
        let b1 = Arc::new(Barrier::new(2));
        let b2 = b1.clone();
        thread::spawn(move || {
            b1.wait();
            thread::sleep(Duration::from_micros(10));
            send.send(Tester(2)).unwrap()
        });
        b2.wait();
        assert_eq!(
            manager.get().collect::<Vec<_>>(),
            vec![Tester(3), Tester(2), Tester(1)]
        );
    }

    #[test]
    fn error_retry_next() {
        let pollable = PollableSource::<Tester> {
            poll: Box::new(move || Err(PollError::RetryNext)),
            min_interval: Duration::from_millis(1),
            preferred_interval: Duration::from_millis(2),
        };
        let mut manager = SourceManager {
            queue: util::prioritized_mpsc::channel().1,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_secs(60),
                pollable,
            }],
        };
        assert_eq!(manager.get().collect::<Vec<_>>(), vec![]);
        let before = Instant::now();
        assert_eq!(manager.get().collect::<Vec<_>>(), vec![]);
        assert!(
            Instant::now().duration_since(before) < Duration::from_micros(100),
            "duration only : {:?}",
            Instant::now().duration_since(before)
        );
    }

    #[test]
    fn error_reset_interval() {
        let pollable = PollableSource::<Tester> {
            poll: Box::new(move || Err(PollError::ResetInterval)),
            min_interval: Duration::from_micros(50),
            preferred_interval: Duration::from_micros(100),
        };
        let mut manager = SourceManager {
            queue: util::prioritized_mpsc::channel().1,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_secs(60),
                pollable,
            }],
        };
        assert_eq!(manager.get().collect::<Vec<_>>(), vec![]);
        let before = Instant::now();
        assert_eq!(manager.get().collect::<Vec<_>>(), vec![]);
        assert!(
            Instant::now().duration_since(before) > Duration::from_micros(100),
            "duration only : {:?}",
            Instant::now().duration_since(before)
        );
    }

    // todo round-robin
}
