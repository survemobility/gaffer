use std::{
    borrow::{Borrow, BorrowMut},
    sync::Arc,
};

use gaffer_queue::PriorityQueue;
use gaffer_runner::WorkerPool;

use crate::{
    source::{PrioritisedJob, RecurringJob, SourceManager},
    Job,
};

/// Callback function to determine the maximum number of threads that could be occupied after a job of a particular priority level was executed
pub(crate) type ConcurrencyLimitFn<J> = dyn Fn(<J as Job>::Priority) -> Option<u8> + Send + Sync;

/// Spawn runners on `thread_num` threads, executing jobs from `jobs` and obeying the concurrency limit `concurrency_limit`
pub(crate) fn spawn<J, R: RecurringJob<Job = J> + Send + 'static>(
    thread_num: usize,
    jobs: SourceManager<J, R>,
    queue: PriorityQueue<PrioritisedJob<J>>,
    concurrency_limit: Arc<ConcurrencyLimitFn<J>>,
) -> WorkerPool
where
    J: Job + 'static,
    <J as Job>::Priority: Send,
{
    gaffer_runner::spawn(
        thread_num,
        Supervisor {
            queue,
            concurrency_limit,
        },
        jobs,
        true,
    )
}

// the supervisor / queue api needs to allow:
// - locking queue for individual steal
// - maybe locking supervisor for an individual load, but it could take care of that itself
// - allow supervisor to lock the queue temporarily during it's loading
// - keeping the queue locked whilst several tasks are dequeued (maybe just figure out how many before, then they can all be dequeued together and passed by value)
// - separate traits for the queue and supervisor would mean they can both be locked by the runner

pub(crate) struct Supervisor<J: Job> {
    queue: PriorityQueue<PrioritisedJob<J>>,
    concurrency_limit: Arc<ConcurrencyLimitFn<J>>,
}

impl<J: Job> Supervisor<J> {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            queue: PriorityQueue::new(),
            concurrency_limit: Arc::new(|_| None),
        }
    }

    pub fn enqueue(&mut self, job: J) {
        self.queue.enqueue(PrioritisedJob(job));
    }
}

impl<J: Job> gaffer_runner::Scheduler<Task<J>> for Supervisor<J> {
    fn steal(&mut self, running: &[Option<J::Exclusion>], limit: usize) -> Vec<Task<J>> {
        log::debug!(
            "Looking for up to {} tasks to execute concurrently with {:?}",
            limit,
            running
        );
        let working_count = running.iter().filter(|state| state.is_some()).count();
        let exclusions: Vec<_> = running.iter().flatten().collect();
        let concurrency_limit = self.concurrency_limit.clone();
        PriorityQueue::drain_where(&mut self.queue, |job| {
            let job = &job.0;
            if let Some(max_concurrency) = (concurrency_limit)(job.priority()) {
                if working_count as u8 >= max_concurrency {
                    return false;
                }
            }
            if exclusions.contains(&&job.exclusion()) {
                return false;
            }
            true
        })
        .map(|PrioritisedJob(job)| Task(job))
        .take(limit)
        .collect()
    }

    fn requeue(&mut self, Task(task): Task<J>) {
        self.queue.enqueue(PrioritisedJob(task));
    }
}

impl<J: Job> Borrow<PriorityQueue<PrioritisedJob<J>>> for Supervisor<J> {
    fn borrow(&self) -> &PriorityQueue<PrioritisedJob<J>> {
        &self.queue
    }
}

impl<J: Job> BorrowMut<PriorityQueue<PrioritisedJob<J>>> for Supervisor<J> {
    fn borrow_mut(&mut self) -> &mut PriorityQueue<PrioritisedJob<J>> {
        &mut self.queue
    }
}

pub(crate) struct Task<J: Job>(pub J);

impl<J> gaffer_runner::Task for Task<J>
where
    J: Job,
{
    type Key = J::Exclusion;

    fn key(&self) -> Self::Key {
        self.0.exclusion()
    }

    fn execute(self) {
        self.0.execute()
    }
}

#[cfg(test)]
mod runner_test {
    use std::{thread, time::Duration};

    use parking_lot::Mutex;
    use time::OffsetDateTime;

    use crate::NoExclusion;

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum Event<J> {
        Start(J),
        End(J),
    }

    #[derive(Debug)]
    struct ExcludedJob(u8, Arc<Mutex<Vec<Event<u8>>>>);

    impl Job for ExcludedJob {
        type Exclusion = u8;

        fn exclusion(&self) -> Self::Exclusion {
            self.0
        }

        type Priority = ();

        fn priority(&self) -> Self::Priority {}

        fn execute(self) {
            log::trace!(
                "{} Executing job {:?}",
                OffsetDateTime::now_utc().time(),
                self.0
            );
            self.1.lock().push(Event::Start(self.0));
            thread::sleep(Duration::from_millis(10));
            self.1.lock().push(Event::End(self.0));
            log::trace!(
                "{} Completed job {}",
                OffsetDateTime::now_utc().time(),
                self.0
            );
        }
    }

    struct PrioritisedJob(u8, Arc<Mutex<Vec<Event<u8>>>>);

    impl Job for PrioritisedJob {
        type Exclusion = NoExclusion;

        fn exclusion(&self) -> Self::Exclusion {
            NoExclusion
        }

        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0
        }

        fn execute(self) {
            log::trace!(
                "{} Executing job {:?}",
                OffsetDateTime::now_utc().time(),
                self.0
            );
            self.1.lock().push(Event::Start(self.0));
            thread::sleep(Duration::from_millis(10));
            self.1.lock().push(Event::End(self.0));
            log::trace!(
                "{} Completed job {}",
                OffsetDateTime::now_utc().time(),
                self.0
            );
        }
    }

    /// exclusion prevents exclusive jobs from running at the same time
    #[test]
    fn working_to_supervisor_excluded() {
        simple_logger::SimpleLogger::new().init().unwrap();

        let queue = PriorityQueue::new();
        let events = Arc::new(Mutex::new(vec![]));
        let (sender, sources) =
            SourceManager::<_, Box<dyn RecurringJob<Job = ExcludedJob> + Send>>::new(vec![], None);
        let pool = spawn(2, sources, queue, Arc::new(|()| None));

        thread::sleep(Duration::from_millis(10));
        sender.send(ExcludedJob(1, events.clone())).unwrap();
        thread::sleep(Duration::from_millis(1));
        sender.send(ExcludedJob(1, events.clone())).unwrap();
        sender.send(ExcludedJob(2, events.clone())).unwrap();

        thread::sleep(Duration::from_millis(100));
        drop(pool);
        assert_eq!(
            *events.lock(),
            vec![
                Event::Start(1),
                Event::Start(2),
                Event::End(1),
                Event::Start(1), // sometimes these 2 are the wrong way round
                Event::End(2),   // suggesting there is a extra delay between End(1) amd Start(1)
                Event::End(1)
            ]
        );
    }

    /// if a job completes and there is another job, but it is throttled to , another job is not taken
    /// fails due to slow unpredictable start up time for a job getting scheduled on another thread
    #[test]
    fn working_to_supervisor_throttled() {
        let events = Arc::new(Mutex::new(vec![]));
        let queue = PriorityQueue::new();
        let (sender, sources) = SourceManager::<
            _,
            Box<dyn RecurringJob<Job = PrioritisedJob> + Send>,
        >::new(vec![], None);
        let pool = spawn(2, sources, queue, Arc::new(|priority| Some(priority)));

        thread::sleep(Duration::from_millis(10));
        sender.send(PrioritisedJob(1, events.clone())).unwrap();
        thread::sleep(Duration::from_micros(10));
        sender.send(PrioritisedJob(1, events.clone())).unwrap();
        sender.send(PrioritisedJob(2, events.clone())).unwrap();

        thread::sleep(Duration::from_millis(100));
        drop(pool);
        log::trace!("{} Stopping and checking", OffsetDateTime::now_utc().time(),);
        assert_eq!(
            *events.lock(),
            vec![
                Event::Start(1),
                Event::Start(2),
                Event::End(1), // FIXME sometimes completes 2 before 1
                Event::End(2),
                Event::Start(1),
                Event::End(1)
            ]
        );
    }
}
