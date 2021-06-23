use std::{iter::Peekable, sync::mpsc::*, thread, time::{Duration, Instant}};

use crate::{Job, Prioritised};

use self::util::Drain;

mod util;

struct SourceManager<J: Prioritised> {
    queue: util::prioritized_mpsc::Receiver<J>,
    sources: Vec<PollSource<J>>,
}

impl<J: Prioritised + 'static> SourceManager<J> {
    fn get(&mut self) -> Iter<'_, J>  {
        let queue = self.queue.iter_timeout(Duration::from_millis(1)).ok();
        let mut polls = Vec::with_capacity(self.sources.len());
        for PollSource {
            pollable,
            last_poll,
        } in &mut self.sources {
            let interval_elapsed = Instant::now().duration_since(*last_poll);
            if interval_elapsed < pollable.preferred_interval {
                thread::sleep(pollable.preferred_interval - interval_elapsed);
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
        Iter {
            queue,
            polls,
        }
    }
}

struct Iter<'m, J: Prioritised> {
    queue: Option<Drain<'m, J>>,
    polls: Vec<Peekable<Box<dyn Iterator<Item = J> + 'm>>>,
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

struct PollSource<J: Prioritised> {
    pollable: PollableSource<J>,
    last_poll: Instant,
}

struct PollableSource<J> {
    poll: Box<dyn FnMut() -> Result<Box<dyn Iterator<Item = J>>, PollError>>,
    /// Minimum interval between polls, if the manager is looking for jobs before this is up, this source will be skipped
    min_interval: Duration,
    /// Preferred interval between polls, if the manager is idle, it will schedule a poll on or before this interval
    preferred_interval: Duration,
}

impl<J> PollableSource<J> {}

enum PollError {
    /// No result was found for the poll, but the minimum poll interval should be observed before polling again
    ResetInterval,
    /// No result was found for the poll, continue for now but leave this source ready to poll
    RetryNext,
}

#[cfg(test)]
mod test {
    use std::time::Duration;

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
            vec![Tester(1), Tester(2), Tester(3)]
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
            },
            ],
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
            },
            ],
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
    fn priority_order_across_sources() {
        let (send, recv) = util::prioritized_mpsc::channel();
        let pollable = PollableSource {
            poll: Box::new(move || Ok(Box::new(vec![Tester(1), Tester(2), Tester(3)].into_iter()))),
            min_interval: Duration::from_micros(100),
            preferred_interval: Duration::from_millis(1),
        };
        send.send(Tester(2)).unwrap();
        let mut manager = SourceManager {
            queue: recv,
            sources: vec![PollSource {
                last_poll: Instant::now() - Duration::from_secs(60),
                pollable,
            },
            ],
        };
        assert_eq!(
            manager.get().collect::<Vec<_>>(),
            vec![Tester(1), Tester(2), Tester(2), Tester(3)]
        );

    }

    // todo test round-robin and intervals
}
