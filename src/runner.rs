use std::{
    fmt::Debug,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use crate::{Job, Priority};

struct RunnerState<J: Job> {
    workers: Arc<Mutex<Vec<WorkerState<J>>>>,
    worker_index: usize,
}

impl<J: Job> RunnerState<J> {
    /// perform state transition after a job has been completed (or to start a worker)
    /// returns job receiver if this worker goes back to being available, or `None` if it becomes the supervisor
    ///
    /// Panics if worker was not either working or not started
    fn completed_job(&self) -> Option<mpsc::Receiver<J>> {
        let mut workers = self.workers.lock().unwrap();
        assert!(
            workers[self.worker_index].is_working() || workers[self.worker_index].is_not_started()
        );
        if workers.iter().any(|worker| worker.is_supervisor()) {
            let (send, recv) = mpsc::channel();
            workers[self.worker_index] = WorkerState::Available(send);
            Some(recv)
        } else {
            workers[self.worker_index] = WorkerState::Supervisor;
            None
        }
    }

    /// assigns jobs to available workers, changing those workers into the `Working` state.
    /// jobs are allocated to workers in order. jobs which clash with running exclusions are skipped. jobs whose priorities indicate a max number of threads below the number of working threads are skipped.
    /// skipped threads are dropped
    /// calls `assigned()` on assigned jobs
    /// if there are still more jobs than available workers, the supervisor will also become a worker and the function returns the job it should execute
    /// unassigned jobs are not consumed
    ///
    /// panics if this worker is not the supervisor
    fn assign_jobs(&self, jobs: impl IntoIterator<Item = J>) -> Option<J> {
        let mut workers = self.workers.lock().unwrap();
        assert!(workers[self.worker_index].is_supervisor());
        let mut exclusions: Vec<_> = workers.iter().flat_map(|state| state.exclusion()).collect();
        let mut working_count = workers.iter().filter(|state| state.is_working()).count();
        let mut workers_iter = workers.iter_mut();
        for mut job in jobs {
            if let Some(max_parallelism) = job.priority().parrallelism() {
                if working_count as u8 >= max_parallelism {
                    continue;
                }
            }
            if exclusions.contains(&job.exclusion()) {
                continue;
            }
            working_count += 1;
            exclusions.push(job.exclusion());
            loop {
                if let Some(worker) = workers_iter.next() {
                    if let WorkerState::Available(send) = worker {
                        let exclusion = job.exclusion();
                        job.assigned();
                        send.send(job).unwrap();
                        *worker = WorkerState::Working(exclusion);
                        break;
                    } else {
                        continue;
                    }
                } else {
                    // no available worker for this job, supervisor to become worker
                    workers[self.worker_index] = WorkerState::Working(job.exclusion());
                    job.assigned();
                    return Some(job);
                }
            }
        }
        None
    }

    fn is_warming_up(&self) -> bool {
        let workers = self.workers.lock().unwrap();
        workers.iter().any(|w| w.is_not_started())
    }
}

#[derive(Debug)]
enum WorkerState<J: Job> {
    NotStarted,
    Supervisor,
    Working(J::Exclusion),
    Available(mpsc::Sender<J>),
}

impl<J: Job> WorkerState<J> {
    /// if worker is working, returns the exclusion, otherwise `None`
    fn exclusion(&self) -> Option<J::Exclusion> {
        if let Self::Working(exclusion) = self {
            Some(*exclusion)
        } else {
            None
        }
    }

    fn is_not_started(&self) -> bool {
        if let Self::NotStarted = self {
            true
        } else {
            false
        }
    }

    fn is_working(&self) -> bool {
        if let Self::Working(_) = self {
            true
        } else {
            false
        }
    }

    fn is_available(&self) -> bool {
        if let Self::Available(_) = self {
            true
        } else {
            false
        }
    }

    fn is_supervisor(&self) -> bool {
        if let Self::Supervisor = self {
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{self, AtomicBool};

    use crate::{Job, NoExclusion, Prioritised, Priority};

    use super::*;

    struct ExcludedJob(Arc<AtomicBool>, u8);

    impl Job for ExcludedJob {
        type Exclusion = u8;

        fn exclusion(&self) -> Self::Exclusion {
            self.1
        }

        fn assigned(&mut self) {
            self.0.store(true, atomic::Ordering::SeqCst);
        }

        fn execute(self) {}
    }

    impl Prioritised for ExcludedJob {
        type Priority = ();

        fn priority(&self) -> Self::Priority {}
    }

    #[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
    struct TestPriority(u8);
    impl Priority for TestPriority {
        fn parrallelism(&self) -> Option<u8> {
            Some(self.0)
        }
    }

    struct PrioritisedJob(Arc<AtomicBool>, u8);

    impl Job for PrioritisedJob {
        type Exclusion = NoExclusion;

        fn exclusion(&self) -> Self::Exclusion {
            NoExclusion
        }

        fn assigned(&mut self) {
            self.0.store(true, atomic::Ordering::SeqCst);
        }

        fn execute(self) {}
    }

    impl Prioritised for PrioritisedJob {
        type Priority = TestPriority;

        fn priority(&self) -> Self::Priority {
            TestPriority(self.1)
        }
    }

    /// if a job completes and there is another supervisor, this worker becomes available
    #[test]
    fn not_started_to_available() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::NotStarted,
                WorkerState::Supervisor,
            ])),
            worker_index: 0,
        };
        assert!(state.is_warming_up());
        let job_recv = state.completed_job();
        assert!(job_recv.is_some());
        {
            let workers = state.workers.lock().unwrap();
            assert!(workers[0].is_available());
        }
        assert!(!state.is_warming_up());
    }

    /// when the first runner starts it should become a supervisor
    #[test]
    fn not_started_to_supervisor() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::NotStarted,
                WorkerState::NotStarted,
            ])),
            worker_index: 0,
        };
        assert!(state.is_warming_up());
        let job_recv = state.completed_job();
        assert!(job_recv.is_none());
        {
            let workers = state.workers.lock().unwrap();
            assert!(workers[0].is_supervisor());
        }
        assert!(state.is_warming_up());
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
        };
        let job_recv = state.completed_job();
        assert!(job_recv.is_some());
        let workers = state.workers.lock().unwrap();
        assert!(workers[0].is_available());
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
        };
        let job_recv = state.completed_job();
        assert!(job_recv.is_none());
        let workers = state.workers.lock().unwrap();
        assert!(workers[0].is_supervisor());
    }

    /// when a job is assigned the state is switched to working and the job is sent over the channel
    #[test]
    fn available_to_working() {
        let assigned = Arc::new(atomic::AtomicBool::new(false));
        let (send, recv) = mpsc::channel();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Available(send),
            ])),
            worker_index: 0,
        };
        assert!(state
            .assign_jobs(vec![ExcludedJob(assigned.clone(), 1)])
            .is_none());
        let workers = state.workers.lock().unwrap();
        assert!(workers[0].is_supervisor());
        assert!(workers[1].is_working());
        assert!(recv.try_recv().is_ok());
        assert!(assigned.load(atomic::Ordering::SeqCst));
    }

    /// if all threads are busy, a supervisor stops supervising and switch to working
    #[test]
    fn supervisor_to_working() {
        let assigned = Arc::new(atomic::AtomicBool::new(false));
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(1),
            ])),
            worker_index: 0,
        };
        assert!(state
            .assign_jobs(vec![ExcludedJob(assigned.clone(), 2)])
            .is_some());
        let workers = state.workers.lock().unwrap();
        assert!(workers[0].is_working());
        assert!(workers[1].is_working());
        assert!(assigned.load(atomic::Ordering::SeqCst));
    }

    /// if a job's exclusion is equal to a running job, it should not be assigned
    #[test]
    fn equal_exclusion_running() {
        let assigned = Arc::new(atomic::AtomicBool::new(false));
        let (send, recv) = mpsc::channel();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(1),
                WorkerState::Available(send),
            ])),
            worker_index: 0,
        };
        assert!(state
            .assign_jobs(vec![ExcludedJob(assigned.clone(), 1)])
            .is_none());
        {
            let workers = state.workers.lock().unwrap();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
            assert!(workers[2].is_available());
        }
        assert!(!assigned.load(atomic::Ordering::SeqCst));
        assert!(recv.try_recv().is_err());
    }

    /// if 2 jobs are added with the same exclusion, only the first should be added
    #[test]
    fn equal_exclusion_adding() {
        let assigned1 = Arc::new(atomic::AtomicBool::new(false));
        let assigned2 = Arc::new(atomic::AtomicBool::new(false));
        let (send, recv) = mpsc::channel();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Available(send.clone()),
                WorkerState::Available(send),
            ])),
            worker_index: 0,
        };
        assert!(state
            .assign_jobs(vec![
                ExcludedJob(assigned1.clone(), 1),
                ExcludedJob(assigned2.clone(), 1)
            ])
            .is_none());
        {
            let workers = state.workers.lock().unwrap();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
            assert!(workers[2].is_available());
        }
        assert!(assigned1.load(atomic::Ordering::SeqCst));
        assert!(!assigned2.load(atomic::Ordering::SeqCst));
        assert!(recv.try_recv().is_ok());
        assert!(recv.try_recv().is_err());
    }

    /// a job with parrallelisation 1 won't be run if a worker is already working
    #[test]
    fn parallelisation_1_running_1() {
        let assigned = Arc::new(atomic::AtomicBool::new(false));
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
            ])),
            worker_index: 0,
        };
        assert!(state
            .assign_jobs(vec![PrioritisedJob(assigned.clone(), 1)])
            .is_none());
        {
            let workers = state.workers.lock().unwrap();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
        }
        assert!(!assigned.load(atomic::Ordering::SeqCst));
    }

    /// a job with parrallelisation 2 will be run if 1 worker is already working
    #[test]
    fn parallelisation_2_running_1() {
        let assigned = Arc::new(atomic::AtomicBool::new(false));
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
            ])),
            worker_index: 0,
        };
        assert!(state
            .assign_jobs(vec![PrioritisedJob(assigned.clone(), 2)])
            .is_some());
        {
            let workers = state.workers.lock().unwrap();
            assert!(workers[0].is_working());
            assert!(workers[1].is_working());
        }
        assert!(assigned.load(atomic::Ordering::SeqCst));
    }

    /// only one job with parrallelisation 2 will be run if 1 worker is already working
    #[test]
    fn parallelisation_2x2_running_1() {
        let assigned1 = Arc::new(atomic::AtomicBool::new(false));
        let assigned2 = Arc::new(atomic::AtomicBool::new(false));
        let (send, recv) = mpsc::channel();
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
                WorkerState::Available(send),
            ])),
            worker_index: 0,
        };
        assert!(state
            .assign_jobs(vec![
                PrioritisedJob(assigned1.clone(), 2),
                PrioritisedJob(assigned2.clone(), 2)
            ])
            .is_none());
        {
            let workers = state.workers.lock().unwrap();
            assert!(workers[0].is_supervisor());
            assert!(workers[1].is_working());
            assert!(workers[2].is_working());
        }
        assert!(assigned1.load(atomic::Ordering::SeqCst));
        assert!(!assigned2.load(atomic::Ordering::SeqCst));
        assert!(recv.try_recv().is_ok());
        assert!(recv.try_recv().is_err());
    }

    #[test]
    fn unassigned_jobs_not_consumed() {
        let assigned1 = Arc::new(atomic::AtomicBool::new(false));
        let assigned2 = Arc::new(atomic::AtomicBool::new(false));
        let mut it = vec![
            PrioritisedJob(assigned1.clone(), 100),
            PrioritisedJob(assigned2.clone(), 100),
        ]
        .into_iter();
        let state = RunnerState::<PrioritisedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(NoExclusion),
            ])),
            worker_index: 0,
        };
        assert!(state.assign_jobs(&mut it).is_some());
        assert_eq!(it.count(), 1);
        assert!(assigned1.load(atomic::Ordering::SeqCst));
        assert!(!assigned2.load(atomic::Ordering::SeqCst));
    }
}

fn run<J: Job, Js: IntoIterator<Item = J>>(
    state: RunnerState<J>,
    jobs: Arc<Mutex<impl FnMut() -> Js>>,
) {
    let mut fresh = true;
    loop {
        let job = if let Some(recv) = state.completed_job() {
            // worker is available
            recv.recv().unwrap()
        } else {
            // worker is supervisor
            // when the runner is warming up, it should let the other workers start before scheduling jobs
            if fresh {
                while state.is_warming_up() {
                    thread::sleep(Duration::from_millis(1));
                }
                fresh = false;
            }

            let mut jobs = jobs.lock().unwrap();
            'supervisor_loop: loop {
                if let Some(job) = state.assign_jobs((jobs)()) {
                    // become a worker
                    break 'supervisor_loop job;
                }
            }
        };
        job.execute();
    }
}
