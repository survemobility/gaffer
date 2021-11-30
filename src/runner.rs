use gaffer_queue::PriorityQueue;
use parking_lot::{Mutex, MutexGuard};
use std::{
    fmt::Debug,
    iter,
    sync::{Arc, Barrier},
    thread::{self, JoinHandle},
};

use crossbeam_channel::SendError;

use crate::{
    source::{prioritized_mpsc::PrioritisedJob, RecurringJob, SourceManager},
    Job,
};

/// Callback function to determine the maximum number of threads that could be occupied after a job of a particular priority level was executed
pub(crate) type ConcurrencyLimitFn<J> = dyn Fn(<J as Job>::Priority) -> Option<u8> + Send + Sync;

/// Spawn runners on `thread_num` threads, executing jobs from `jobs` and obeying the concurrency limit `concurrency_limit`
pub(crate) fn spawn<J, R: RecurringJob<J> + Send + 'static>(
    thread_num: usize,
    jobs: Arc<Mutex<SourceManager<J, R>>>,
    concurrency_limit: Box<ConcurrencyLimitFn<J>>,
) -> Vec<JoinHandle<()>>
where
    J: Job + 'static,
    <J as Job>::Priority: Send,
{
    let queue = jobs.lock().queue();
    let barrier = Arc::new(Barrier::new(thread_num));
    RunnerState::new(thread_num, concurrency_limit)
        .map(move |(recv, state)| {
            let jobs = jobs.clone();
            let queue = queue.clone();
            let barrier = barrier.clone();
            thread::Builder::new()
                .name(format!("gaffer#{}", state.worker_index))
                .spawn(move || {
                    Runner::new(state, jobs, queue).run(barrier, recv);
                })
                .unwrap()
        })
        .collect()
}

struct Runner<J: Job + 'static, R: RecurringJob<J> + Send + 'static> {
    state: RunnerState<J>,
    jobs: Arc<Mutex<SourceManager<J, R>>>,
    queue: Arc<Mutex<PriorityQueue<PrioritisedJob<J>>>>,
}

impl<J, R> Runner<J, R>
where
    J: Job + 'static,
    R: RecurringJob<J> + Send,
{
    fn new(
        state: RunnerState<J>,
        jobs: Arc<Mutex<SourceManager<J, R>>>,
        queue: Arc<Mutex<PriorityQueue<PrioritisedJob<J>>>>,
    ) -> Self {
        Self { state, jobs, queue }
    }

    /// Run the runner loop, `ready_barrier` syncronizes with the start of the other runners and decides the initial supervisor
    fn run(self, ready_barrier: Arc<Barrier>, recv: crossbeam_channel::Receiver<J>) -> ! {
        let job = if ready_barrier.wait().is_leader() {
            // become the supervisor
            self.state.become_supervisor();
            self.run_supervisor()
        } else {
            // worker is available
            recv.recv()
                .expect("Available worker is not connected to shared runner state")
        };
        drop(recv);
        self.run_worker(job);
    }

    fn run_worker(self, mut job: J) -> ! {
        loop {
            job.execute(); // so a panicking job doesn't kill workers
            job = self.next_job();
        }
    }

    fn next_job(&self) -> J {
        let workers = self.state.workers();
        assert!(workers[self.state.worker_index].is_working());
        let mut exclusions: Vec<_> = workers
            .iter()
            .enumerate()
            .flat_map(|(worker_index, state)| {
                (self.state.worker_index != worker_index)
                    .then(|| state.exclusion())
                    .flatten()
            })
            .collect();
        let mut working_count = workers.iter().filter(|state| state.is_working()).count() - 1; // not including self
        drop(workers);

        let transition = self.state.completed_job(
            self.queue
                .lock()
                .drain_where(|job| {
                    let job = &job.0;
                    if let Some(max_concurrency) = (self.state.concurrency_limit)(job.priority()) {
                        if working_count as u8 >= max_concurrency {
                            return false;
                        }
                    }
                    if exclusions.contains(&job.exclusion()) {
                        return false;
                    }
                    working_count += 1;
                    exclusions.push(job.exclusion());
                    true
                })
                .map(|PrioritisedJob(job)| job),
        );
        match transition {
            PostJobTransition::BecomeAvailable(recv) => recv
                .recv()
                .expect("Available worker is not connected to shared runner state"),
            PostJobTransition::BecomeSupervisor => self.run_supervisor(),
            PostJobTransition::KeepWorking(job) => job,
        }
    }

    /// Run the supervisor loop, jobs are retrieved and assigned. Returns when the supervisor has a job to execute and it becomes a worker
    fn run_supervisor(&self) -> J {
        let mut wait_for_new = false;
        let mut jobs = self.jobs.lock();
        loop {
            let workers = self.state.workers();
            assert!(workers[self.state.worker_index].is_supervisor());
            let mut exclusions: Vec<_> =
                workers.iter().flat_map(|state| state.exclusion()).collect();
            let mut working_count = workers.iter().filter(|state| state.is_working()).count();
            drop(workers);

            if let Some(job) = self.state.assign_jobs(jobs.get(wait_for_new, |job| {
                let job = &job.0;
                if let Some(max_concurrency) = (self.state.concurrency_limit)(job.priority()) {
                    if working_count as u8 >= max_concurrency {
                        return false;
                    }
                }
                if exclusions.contains(&job.exclusion()) {
                    return false;
                }
                working_count += 1;
                exclusions.push(job.exclusion());
                true
            })) {
                // become a worker
                return job;
            }
            wait_for_new = true;
        }
    }

    /// Entry point for a new thread, replacing one which panicked whilst executing a job
    fn panic_recover(self) -> ! {
        let job = self.next_job();
        self.run_worker(job);
    }
}

impl<J: Job + 'static, R: RecurringJob<J> + Send + 'static> Drop for Runner<J, R> {
    fn drop(&mut self) {
        if thread::panicking() {
            // spawn another thread to take over
            let Runner {
                state:
                    RunnerState {
                        workers,
                        worker_index,
                        concurrency_limit,
                    },
                jobs,
                queue,
            } = self;
            let state = RunnerState {
                workers: workers.clone(),
                worker_index: *worker_index,
                concurrency_limit: concurrency_limit.clone(),
            };
            let runner = Runner::new(state, jobs.clone(), queue.clone());
            thread::Builder::new()
                .name(format!("gaffer#{}", worker_index))
                .spawn(move || {
                    runner.panic_recover();
                })
                .unwrap();
        }
    }
}

struct RunnerState<J: Job> {
    workers: Arc<Mutex<Vec<WorkerState<J>>>>,
    worker_index: usize,
    concurrency_limit: Arc<ConcurrencyLimitFn<J>>,
}

impl<J: Job> RunnerState<J> {
    pub fn new(
        num: usize,
        concurrency_limit: impl Into<Arc<ConcurrencyLimitFn<J>>>,
    ) -> impl Iterator<Item = (crossbeam_channel::Receiver<J>, Self)> {
        let (receivers, worker_state): (Vec<_>, _) =
            iter::repeat_with(WorkerState::available).take(num).unzip();
        let worker_state = Arc::new(Mutex::new(worker_state));
        let concurrency_limit = concurrency_limit.into();
        receivers.into_iter().enumerate().map(move |(idx, recv)| {
            (
                recv,
                Self {
                    workers: worker_state.clone(),
                    worker_index: idx,
                    concurrency_limit: concurrency_limit.clone(),
                },
            )
        })
    }

    fn become_supervisor(&self) {
        let mut workers = self.workers();
        assert!(!workers.iter().any(|worker| worker.is_supervisor()));
        workers[self.worker_index] = WorkerState::Supervisor;
    }

    /// perform state transition after a job has been completed
    /// returns job receiver if this worker goes back to being available, or `None` if it becomes the supervisor
    ///
    /// Panics if worker was not either working or not started
    fn completed_job(&self, mut jobs: impl Iterator<Item = J>) -> PostJobTransition<J> {
        let mut workers = self.workers();
        assert!(workers[self.worker_index].is_working());
        log::debug!(
            "{}: Job completed by worker",
            std::thread::current().name().unwrap_or_default()
        );
        if let Some(job) = jobs.next() {
            return PostJobTransition::KeepWorking(job);
        }
        if workers.iter().any(|worker| worker.is_supervisor()) {
            let (send, recv) = crossbeam_channel::bounded(1);
            workers[self.worker_index] = WorkerState::Available(send);
            log::trace!(
                "{}: > Supervisor found, becoming available",
                std::thread::current().name().unwrap_or_default()
            );
            PostJobTransition::BecomeAvailable(recv)
        } else {
            log::trace!(
                "{}: > No supervisor found, becoming supervisor",
                std::thread::current().name().unwrap_or_default()
            );
            workers[self.worker_index] = WorkerState::Supervisor;
            PostJobTransition::BecomeSupervisor
        }
    }

    /// assigns jobs to available workers, changing those workers into the `Working` state.
    /// jobs are allocated to workers in order. jobs which clash with running exclusions are skipped. jobs whose priorities indicate a max number of threads below the number of working threads are skipped.
    /// skipped threads are dropped
    /// if there are still more jobs than available workers, the supervisor will also become a worker and the function returns the job it should execute
    /// unassigned jobs are not consumed
    ///
    /// panics if this worker is not the supervisor
    fn assign_jobs(&self, jobs: impl Iterator<Item = J>) -> Option<J> {
        let mut workers = self.workers();
        assert!(workers[self.worker_index].is_supervisor());
        let working_count = workers.iter().filter(|state| state.is_working()).count();
        log::debug!(
            "{}: Supervisor to assign jobs, {} currently working",
            std::thread::current().name().unwrap_or_default(),
            working_count
        );
        let mut workers_iter = workers.iter_mut();
        for mut job in jobs {
            loop {
                if let Some(worker) = workers_iter.next() {
                    if let WorkerState::Available(send) = worker {
                        let exclusion = job.exclusion();
                        if let Err(SendError(returned_job)) = send.send(job) {
                            job = returned_job; // if a worker has died, the rest of the workers can continue
                        } else {
                            *worker = WorkerState::Working(exclusion);
                            break;
                        }
                    } else {
                        continue;
                    }
                } else {
                    // no available worker for this job, supervisor to become worker
                    workers[self.worker_index] = WorkerState::Working(job.exclusion());
                    return Some(job);
                }
            }
        }
        None
    }

    fn workers(&self) -> MutexGuard<'_, Vec<WorkerState<J>>> {
        self.workers.lock()
    }
}

#[derive(Debug)]
enum PostJobTransition<J> {
    BecomeSupervisor,
    BecomeAvailable(crossbeam_channel::Receiver<J>),
    KeepWorking(J),
}

#[derive(Debug)]
enum WorkerState<J: Job> {
    Supervisor,
    Working(J::Exclusion),
    Available(crossbeam_channel::Sender<J>),
}

impl<J: Job> WorkerState<J> {
    fn available() -> (crossbeam_channel::Receiver<J>, Self) {
        let (send, recv) = crossbeam_channel::bounded(1);
        (recv, Self::Available(send))
    }

    /// if worker is working, returns the exclusion, otherwise `None`
    fn exclusion(&self) -> Option<J::Exclusion> {
        if let Self::Working(exclusion) = self {
            Some(*exclusion)
        } else {
            None
        }
    }

    fn is_working(&self) -> bool {
        matches!(self, Self::Working(_))
    }

    fn is_supervisor(&self) -> bool {
        matches!(self, Self::Supervisor)
    }
}

#[cfg(test)]
mod runner_state_test {
    use crate::{Job, NoExclusion};

    use super::*;

    #[derive(Debug)]
    struct ExcludedJob(u8);

    impl Job for ExcludedJob {
        type Exclusion = u8;

        fn exclusion(&self) -> Self::Exclusion {
            self.0
        }

        type Priority = ();

        fn priority(&self) -> Self::Priority {}

        fn execute(self) {}
    }

    struct PrioritisedJob(u8);

    impl Job for PrioritisedJob {
        type Exclusion = NoExclusion;

        fn exclusion(&self) -> Self::Exclusion {
            NoExclusion
        }

        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0
        }

        fn execute(self) {}
    }

    /// if a job completes and there is another supervisor, this worker becomes available
    #[test]
    fn working_to_available() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(1),
                WorkerState::Supervisor,
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        let job_recv = state.completed_job(iter::empty());
        assert!(matches!(job_recv, PostJobTransition::BecomeAvailable(_)));
        let workers = state.workers.lock();
        assert!(matches!(workers[0], WorkerState::Available(_)));
    }

    /// if a job completes and there is no other supervisor, this worker becomes a supervisor
    #[test]
    fn working_to_supervisor() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(1),
                WorkerState::Working(2),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        let job_recv = state.completed_job(iter::empty());
        assert!(matches!(job_recv, PostJobTransition::BecomeSupervisor));
        let workers = state.workers.lock();
        assert!(workers[0].is_supervisor());
    }

    /// if a job completes and there is another job, this worker remains a worker
    #[test]
    fn working_to_working() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(1),
                WorkerState::Working(2),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        let job_recv = state.completed_job(iter::once(ExcludedJob(3)));
        assert!(
            matches!(job_recv, PostJobTransition::KeepWorking(ExcludedJob(3))),
            "{:?}",
            job_recv
        );
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
    }

    /// when a job is assigned the state is switched to working and the job is sent over the channel
    #[test]
    fn available_to_working() {
        let (send, recv) = crossbeam_channel::unbounded();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Available(send),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        assert!(state.assign_jobs(iter::once(ExcludedJob(1))).is_none());
        let workers = state.workers.lock();
        assert!(workers[0].is_supervisor());
        assert!(workers[1].is_working());
        assert!(recv.try_recv().is_ok());
    }

    /// if all threads are busy, a supervisor stops supervising and switch to working
    #[test]
    fn supervisor_to_working() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(1),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        assert!(state.assign_jobs(iter::once(ExcludedJob(2))).is_some());
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
        assert!(workers[1].is_working());
    }
}

#[cfg(test)]
mod runner_test {
    use std::time::Duration;

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
            self.1.lock().push(Event::Start(self.0));
            thread::sleep(Duration::from_millis(2));
            self.1.lock().push(Event::End(self.0));
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
            self.1.lock().push(Event::Start(self.0));
            thread::sleep(Duration::from_millis(2));
            self.1.lock().push(Event::End(self.0));
        }
    }

    /// exclusion prevents exclusive jobs from running at the same time
    #[test]
    fn working_to_supervisor_excluded() {
        let events = Arc::new(Mutex::new(vec![]));
        let (sender, sources) =
            SourceManager::<ExcludedJob, Box<dyn RecurringJob<ExcludedJob> + Send>>::new();
        let jobs = Arc::new(Mutex::new(sources));
        let threads = spawn(2, jobs, Box::new(|()| None));

        thread::sleep(Duration::from_millis(10));
        sender.send(ExcludedJob(1, events.clone())).unwrap();
        thread::sleep(Duration::from_micros(10));
        sender.send(ExcludedJob(1, events.clone())).unwrap();
        sender.send(ExcludedJob(2, events.clone())).unwrap();

        thread::sleep(Duration::from_millis(100)); // better to drain
        assert_eq!(
            *events.lock(),
            vec![
                Event::Start(1),
                Event::Start(2),
                Event::End(1),
                Event::Start(1),
                Event::End(2),
                Event::End(1)
            ]
        );
    }

    /// if a job completes and there is another job, but it is throttled to , another job is not taken
    /// fails due to slow unpredictable start up time for a job getting scheduled on another thread
    #[test]
    fn working_to_supervisor_throttled() {
        let events = Arc::new(Mutex::new(vec![]));
        let (sender, sources) =
            SourceManager::<PrioritisedJob, Box<dyn RecurringJob<PrioritisedJob> + Send>>::new();
        let jobs = Arc::new(Mutex::new(sources));
        let threads = spawn(2, jobs, Box::new(|priority| Some(priority)));

        thread::sleep(Duration::from_millis(10));
        sender.send(PrioritisedJob(1, events.clone())).unwrap();
        thread::sleep(Duration::from_micros(10));
        sender.send(PrioritisedJob(1, events.clone())).unwrap();
        sender.send(PrioritisedJob(2, events.clone())).unwrap();

        thread::sleep(Duration::from_millis(100)); // TODO better to drain
        assert_eq!(
            *events.lock(),
            vec![
                Event::Start(1),
                Event::Start(2),
                Event::End(1),
                Event::End(2),
                Event::Start(1),
                Event::End(1)
            ]
        );
    }
}
