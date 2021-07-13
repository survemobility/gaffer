use parking_lot::{Mutex, MutexGuard};
use std::{
    fmt::Debug,
    iter,
    panic::{self, UnwindSafe},
    sync::{Arc, Barrier},
    thread::{self, JoinHandle},
};

use crossbeam_channel::SendError;

use crate::{
    source::{
        util::{may_be_taken::SkipIterator, PriorityQueue},
        RecurringJob, SourceManager,
    },
    Job, Prioritised,
};

/// Callback function to determine the maximum number of threads that could be occupied after a job of a particular priority level was executed
pub(crate) type ConcurrencyLimitFn<J> =
    dyn Fn(<J as Prioritised>::Priority) -> Option<u8> + Send + Sync;

/// Spawn runners on `thread_num` threads, executing jobs from `jobs` and obeying the concurrency limit `concurrency_limit`
pub fn spawn<J, R: RecurringJob<J> + Send + 'static>(
    thread_num: usize,
    jobs: Arc<Mutex<SourceManager<J, R>>>,
    concurrency_limit: Box<ConcurrencyLimitFn<J>>,
) -> Vec<JoinHandle<()>>
where
    J: Job + UnwindSafe + 'static,
    <J as Prioritised>::Priority: Send,
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
                    run(state, jobs, queue, barrier, recv);
                })
                .unwrap()
        })
        .collect()
}

/// Run the runner loop, `ready_barrier` syncronizes with the start of the other runners and decides the initial supervisor
fn run<J: Job + UnwindSafe + 'static, R: RecurringJob<J>>(
    state: RunnerState<J>,
    jobs: Arc<Mutex<SourceManager<J, R>>>,
    queue: Arc<Mutex<PriorityQueue<J>>>,
    ready_barrier: Arc<Barrier>,
    recv: crossbeam_channel::Receiver<J>,
) -> ! {
    let mut job = if ready_barrier.wait().is_leader() {
        // become the supervisor
        state.become_supervisor();
        run_supervisor(&state, &jobs)
    } else {
        // worker is available
        recv.recv()
            .expect("Available worker is not connected to shared runner state")
    };
    drop(recv);
    loop {
        let _ = panic::catch_unwind(|| job.execute()); // so a panicking job doesn't kill workers
        let transition = state.completed_job(queue.lock().drain());
        job = match transition {
            PostJobTransition::BecomeAvailable(recv) => recv
                .recv()
                .expect("Available worker is not connected to shared runner state"),
            PostJobTransition::BecomeSupervisor => run_supervisor(&state, &jobs),
            PostJobTransition::KeepWorking(job) => job,
        };
    }
}

/// Run the supervisor loop, jobs are retrieved and assigned. Returns when the supervisor has a job to execute and it becomes a worker
fn run_supervisor<J: Job + 'static, R: RecurringJob<J>>(
    state: &RunnerState<J>,
    jobs: &Arc<Mutex<SourceManager<J, R>>>,
) -> J {
    let mut wait_for_new = false;
    let mut jobs = jobs.lock();
    loop {
        if let Some(job) = state.assign_jobs(jobs.get(wait_for_new)) {
            // become a worker
            return job;
        }
        wait_for_new = true;
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
    fn completed_job(&self, mut jobs: impl SkipIterator<Item = J>) -> PostJobTransition<J> {
        let mut workers = self.workers();
        assert!(workers[self.worker_index].is_working());
        log::debug!(
            "{}: Job completed by worker",
            std::thread::current().name().unwrap_or_default()
        );
        let working_count = workers.iter().filter(|state| state.is_working()).count() - 1; // not including self
        while let Some(job) = jobs.maybe_next() {
            if let Some(max_concurrency) = (self.concurrency_limit)(job.priority()) {
                if working_count as u8 >= max_concurrency {
                    log::trace!(
                        "{}: > Can't continue onto this job as {} working and {} max concurrency",
                        std::thread::current().name().unwrap_or_default(),
                        working_count,
                        max_concurrency
                    );
                    continue;
                }
            }
            if workers
                .iter()
                .any(|worker| worker.exclusion() == Some(job.exclusion()))
            {
                log::trace!(
                    "{}: > Can't continue onto this job as exclusion matches",
                    std::thread::current().name().unwrap_or_default()
                );
                continue;
            }
            return PostJobTransition::KeepWorking(job.into_inner());
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
    fn assign_jobs(&self, mut jobs: impl SkipIterator<Item = J>) -> Option<J> {
        let mut workers = self.workers();
        assert!(workers[self.worker_index].is_supervisor());
        let mut exclusions: Vec<_> = workers.iter().flat_map(|state| state.exclusion()).collect();
        let mut working_count = workers.iter().filter(|state| state.is_working()).count();
        log::debug!(
            "{}: Supervisor to assign jobs, {} currently working",
            std::thread::current().name().unwrap_or_default(),
            working_count
        );
        let mut workers_iter = workers.iter_mut();
        while let Some(job) = jobs.maybe_next() {
            if let Some(max_concurrency) = (self.concurrency_limit)(job.priority()) {
                if working_count as u8 >= max_concurrency {
                    continue;
                }
            }
            if exclusions.contains(&job.exclusion()) {
                continue;
            }
            working_count += 1;
            exclusions.push(job.exclusion());
            let mut job = job.into_inner();
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
mod test {
    use crate::{source::util::may_be_taken::VecSkipIter, Job, NoExclusion, Prioritised};

    use super::*;

    #[derive(Debug)]
    struct ExcludedJob(u8);

    impl Job for ExcludedJob {
        type Exclusion = u8;

        fn exclusion(&self) -> Self::Exclusion {
            self.0
        }

        fn execute(self) {}
    }

    impl Prioritised for ExcludedJob {
        type Priority = ();

        fn priority(&self) -> Self::Priority {}
    }

    struct PrioritisedJob(u8);

    impl Job for PrioritisedJob {
        type Exclusion = NoExclusion;

        fn exclusion(&self) -> Self::Exclusion {
            NoExclusion
        }

        fn execute(self) {}
    }

    impl Prioritised for PrioritisedJob {
        type Priority = u8;

        fn priority(&self) -> Self::Priority {
            self.0
        }
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
        let job_recv = state.completed_job(PriorityQueue::new().drain());
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
        let job_recv = state.completed_job(PriorityQueue::new().drain());
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
        let mut queue = PriorityQueue::new();
        queue.enqueue(ExcludedJob(3));
        let job_recv = state.completed_job(queue.drain());
        assert!(
            matches!(job_recv, PostJobTransition::KeepWorking(ExcludedJob(3))),
            "{:?}",
            job_recv
        );
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
        assert!(queue.is_empty());
    }

    /// if a job completes and there is another job, but it is excluded, another job is not taken
    #[test]
    fn working_to_supervisor_excluded() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(1),
                WorkerState::Working(2),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        let mut queue = PriorityQueue::new();
        queue.enqueue(ExcludedJob(1));
        let job_recv = state.completed_job(queue.drain());
        assert!(matches!(job_recv, PostJobTransition::BecomeSupervisor));
        let workers = state.workers.lock();
        assert!(workers[0].is_supervisor());
        assert!(!queue.is_empty());
    }

    /// if a job completes and there is another job, but it is throttled to , another job is not taken
    #[test]
    fn working_to_supervisor_throttled() {
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(NoExclusion),
                WorkerState::Working(NoExclusion),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|num| Some(num)),
        };
        let mut queue = PriorityQueue::new();
        queue.enqueue(PrioritisedJob(1));
        let job_recv = state.completed_job(queue.drain());
        assert!(matches!(job_recv, PostJobTransition::BecomeSupervisor));
        let workers = state.workers.lock();
        assert!(workers[0].is_supervisor());
        assert!(!queue.is_empty());
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
        let mut jobs = vec![ExcludedJob(1)];
        assert!(state.assign_jobs(VecSkipIter::new(&mut jobs)).is_none());
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
        assert!(state
            .assign_jobs(VecSkipIter::new(&mut vec![ExcludedJob(2)]))
            .is_some());
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
        assert!(workers[1].is_working());
    }

    /// if a job's exclusion is equal to a running job, it should not be assigned
    #[test]
    fn equal_exclusion_running() {
        let (send, recv) = crossbeam_channel::unbounded();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(1),
                WorkerState::Available(send),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        let mut jobs = vec![ExcludedJob(1)];
        assert!(state.assign_jobs(VecSkipIter::new(&mut jobs)).is_none());
        {
            let workers = state.workers.lock();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
            assert!(matches!(workers[2], WorkerState::Available(_)));
        }
        assert!(recv.try_recv().is_err());
        assert_eq!(jobs.len(), 1);
    }

    /// if 2 jobs are added with the same exclusion, only the first should be added
    #[test]
    fn equal_exclusion_adding() {
        let (send, recv) = crossbeam_channel::unbounded();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Available(send.clone()),
                WorkerState::Available(send),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|()| None),
        };
        let mut jobs = vec![ExcludedJob(1), ExcludedJob(1)];
        assert!(state.assign_jobs(VecSkipIter::new(&mut jobs)).is_none());
        {
            let workers = state.workers.lock();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
            assert!(matches!(workers[2], WorkerState::Available(_)));
        }
        assert!(recv.try_recv().is_ok());
        assert!(recv.try_recv().is_err());
        assert_eq!(jobs.len(), 1);
    }

    /// a job with parrallelisation 1 won't be run if a worker is already working
    #[test]
    fn parallelisation_1_running_1() {
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|priority| Some(priority)),
        };
        let mut jobs = vec![PrioritisedJob(1)];
        assert!(state.assign_jobs(VecSkipIter::new(&mut jobs)).is_none());
        {
            let workers = state.workers.lock();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
        }
        assert_eq!(jobs.len(), 1);
    }

    /// a job with parrallelisation 2 will be run if 1 worker is already working
    #[test]
    fn parallelisation_2_running_1() {
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|priority| Some(priority)),
        };
        assert!(state
            .assign_jobs(VecSkipIter::new(&mut vec![PrioritisedJob(2)]))
            .is_some());
        {
            let workers = state.workers.lock();
            assert!(workers[0].is_working());
            assert!(workers[1].is_working());
        }
    }

    /// only one job with parrallelisation 2 will be run if 1 worker is already working
    #[test]
    fn parallelisation_2x2_running_1() {
        let (send, recv) = crossbeam_channel::unbounded();
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
                WorkerState::Available(send),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|priority| Some(priority)),
        };
        let mut jobs = vec![PrioritisedJob(2), PrioritisedJob(2)];
        assert!(state.assign_jobs(VecSkipIter::new(&mut jobs)).is_none());
        {
            let workers = state.workers.lock();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
            assert!(workers[2].is_working());
        }
        assert!(recv.try_recv().is_ok());
        assert!(recv.try_recv().is_err());
        assert_eq!(jobs.len(), 1);
    }

    #[test]
    fn unassigned_jobs_not_consumed() {
        let mut jobs = vec![PrioritisedJob(100), PrioritisedJob(100)];
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
            ])),
            worker_index: 0,
            concurrency_limit: Arc::new(|priority| Some(priority)),
        };
        assert!(state.assign_jobs(VecSkipIter::new(&mut jobs)).is_some());
        assert_eq!(jobs.len(), 1);
    }
}
