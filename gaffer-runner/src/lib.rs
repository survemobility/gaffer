//! # The gaffer runner
//!
//! A supervised, fixed size worker thread pool with no thread overhead.
//!
//! The supervision in this thread pool allows for custom scheduling logic of tasks arriving at the pool, and synchronised source management. It is for use cases where you want to restrict some tasks from running when other tasks are running. This necessarily adds some overhead and some synchronisation. It won't be as high-performance as work-stealing, but it's designed for cases where that is not suitable.
//!
//! There are 2 cases where the runner fetches tasks to run:
//! * the supervisor can fetch tasks, this can be blocked if you need to wait for tasks from other threads / the network
//! * (optionally) a worker can steal a task while the supervisor is running, this should not block. This improves performance in the case that a task is ready but was blocked from running by the task already running on this worker. The next task can be scheduled without waiting for the supervisor to finish what it is doing and without the worker thread going to sleep
//!
//!

use parking_lot::{Mutex, MutexGuard};
use std::{
    fmt, iter,
    sync::{
        atomic::{self, AtomicBool},
        Arc, Barrier,
    },
    thread,
};

use crossbeam_channel::SendError;

/// A task which can be executed by the runner, with features to synchronise jobs that would interfere with each other and reduce the parallelisation of low priority jobs
pub trait Task: Send + 'static {
    type Key: PartialEq + Copy + fmt::Debug + Send;

    /// Used to provide info about what tasks are running, can be used by schduler logic to decide which other tasks to schedule
    fn key(&self) -> Self::Key;

    /// Execute and consume the task
    fn execute(self);
}

impl<T> Task for T
where
    T: FnOnce() + Send + 'static,
{
    type Key = ();

    fn key(&self) -> Self::Key {}

    fn execute(self) {
        (self)()
    }
}

/// Spawn runners on `thread_num` threads, executing tasks from `scheduler` and using the task loader `loader`. `restart_on_panic` spawns new threads to replace any killed by panic
pub fn spawn<T, S, L>(
    thread_num: usize,
    scheduler: S,
    loader: L,
    restart_on_panic: bool,
) -> WorkerPool
where
    T: Task + 'static,
    S: Scheduler<T>,
    L: Loader<T, Scheduler = S>,
{
    let scheduler = Arc::new(Mutex::new(scheduler));
    let loader = Arc::new(Mutex::new(loader));
    let barrier = Arc::new(Barrier::new(thread_num));
    let stopping = Arc::new(AtomicBool::new(false));
    for (recv, state) in RunnerState::new(thread_num, stopping.clone()) {
        let barrier = barrier.clone();
        let scheduler = scheduler.clone();
        let loader = loader.clone();
        thread::Builder::new()
            .name(format!("gaffer#{}", state.worker_index))
            .spawn(move || {
                Runner::new(state, scheduler, loader, restart_on_panic).run(barrier, recv);
            })
            .unwrap();
    }
    WorkerPool { stopping }
}

pub struct WorkerPool {
    stopping: Arc<AtomicBool>,
}

impl WorkerPool {
    /// Graciously stop the pool, workers will stop as they become idle, but won't stop until the currently available tasks are completed. Supervisor will keep loading tasks until it goes idle and stops. Returns immediately
    pub fn stop(self) {
        self.stopping.store(true, atomic::Ordering::Relaxed);
    }
}

pub trait Scheduler<T: Task>: Send + 'static {
    ///
    fn steal(&mut self, running: &[Option<T::Key>], limit: usize) -> Vec<T>;
    /// incase the runner takes more than it can schedule, avoiding this was why i had the skip iterator in the first place, reconsider that
    fn requeue(&mut self, task: T);
}

pub trait Loader<T: Task>: Send + 'static {
    type Scheduler: Scheduler<T>;

    fn load(&mut self, idle: bool, scheduler: &Mutex<Self::Scheduler>); // maybe idle can be removed
}

struct Runner<T, S, L>
where
    T: Task + 'static,
    S: Scheduler<T>,
    L: Loader<T, Scheduler = S>,
{
    state: RunnerState<T>,
    scheduler: Arc<Mutex<S>>,
    loader: Arc<Mutex<L>>,
    restart_on_panic: bool,
}

impl<T, S, L> Runner<T, S, L>
where
    T: Task + 'static,
    S: Scheduler<T>,
    L: Loader<T, Scheduler = S>,
{
    fn new(
        state: RunnerState<T>,
        scheduler: Arc<Mutex<S>>,
        loader: Arc<Mutex<L>>,
        restart_on_panic: bool,
    ) -> Self {
        Self {
            state,
            scheduler,
            loader,
            restart_on_panic,
        }
    }

    /// Run the runner loop, `ready_barrier` syncronizes with the start of the other runners and decides the initial supervisor
    fn run(
        self,
        ready_barrier: Arc<Barrier>,
        recv: crossbeam_channel::Receiver<WorkerInstruction<T>>,
    ) {
        let task = if ready_barrier.wait().is_leader() {
            // become the supervisor
            self.state.become_supervisor();
            self.run_supervisor()
        } else {
            // worker is available
            match recv
                .recv()
                .expect("Available worker is not connected to shared runner state")
            {
                WorkerInstruction::Assign(task) => Some(task),
                WorkerInstruction::BecomeSupervisor => self.run_supervisor(), // todo put this on the enum
            }
        };
        drop(recv);
        if let Some(task) = task {
            self.run_worker(task);
        }
    }

    fn run_worker(self, mut task: T) {
        loop {
            task.execute();
            if let Some(t) = self.next_job() {
                task = t;
            } else {
                return;
            }
        }
    }

    // next job for the worker to execute, if None, the worker is being stopped
    fn next_job(&self) -> Option<T> {
        let transition = self
            .state
            .completed_job(|t| self.scheduler.lock().steal(t, 1).pop());
        match transition {
            PostJobTransition::BecomeAvailable(recv) => match recv
                .recv()
                .expect("Available worker is not connected to shared runner state")
            {
                WorkerInstruction::Assign(task) => Some(task),
                WorkerInstruction::BecomeSupervisor => self.run_supervisor(),
            },
            PostJobTransition::BecomeSupervisor => self.run_supervisor(),
            PostJobTransition::KeepWorking(task) => Some(task),
            PostJobTransition::Stop => None,
        }
    }

    /// Run the supervisor loop, jobs are retrieved and assigned. Returns when the supervisor has a task to execute and it becomes a worker
    fn run_supervisor(&self) -> Option<T> {
        log::trace!("{} Became the supervisor ", self.state.worker_index);
        let mut idle = false;
        loop {
            log::trace!("Loading jobs");
            self.loader.lock().load(idle, &*self.scheduler);
            log::trace!("Loaded jobs");
            if let Some(task) = self.state.assign_jobs(&*self.scheduler) {
                return Some(task);
            } else if self.state.stopping.load(atomic::Ordering::Relaxed) {
                return None;
            }
            idle = true; // if no task was assigned to the supervisor in the first round, it is idle and it can look harder / wait for tasks
        }
    }

    /// Entry point for a new thread, replacing one which panicked whilst executing a task
    fn panic_recover(self) {
        if let Some(task) = self.next_job() {
            self.run_worker(task);
        }
    }
}

impl<T, S, L> Drop for Runner<T, S, L>
where
    T: Task + 'static,
    S: Scheduler<T>,
    L: Loader<T, Scheduler = S>,
{
    fn drop(&mut self) {
        if self.restart_on_panic && thread::panicking() {
            log::error!("Worker panicked, restarting");
            // spawn another thread to take over
            let Runner {
                state:
                    RunnerState {
                        workers,
                        worker_index,
                        stopping,
                    },
                scheduler,
                loader,
                restart_on_panic,
            } = self;
            let state = RunnerState {
                workers: workers.clone(),
                worker_index: *worker_index,
                stopping: stopping.clone(),
            };
            let runner = Runner::new(state, scheduler.clone(), loader.clone(), *restart_on_panic);
            thread::Builder::new()
                .name(format!("gaffer#{}", worker_index))
                .spawn(move || {
                    runner.panic_recover();
                })
                .unwrap();
        } else if thread::panicking() {
            log::error!("Worker panicked, `restart_on_panic` not enabled")
        }
    }
}

struct RunnerState<T: Task> {
    workers: Arc<Mutex<Vec<WorkerState<T>>>>,
    worker_index: usize,
    stopping: Arc<AtomicBool>,
}

impl<T: Task> RunnerState<T> {
    pub fn new(
        num: usize,
        stopping: Arc<AtomicBool>,
    ) -> impl Iterator<Item = (crossbeam_channel::Receiver<WorkerInstruction<T>>, Self)> {
        let (receivers, worker_state): (Vec<_>, _) =
            iter::repeat_with(WorkerState::available).take(num).unzip();
        let worker_state = Arc::new(Mutex::new(worker_state));
        receivers.into_iter().enumerate().map(move |(idx, recv)| {
            (
                recv,
                Self {
                    workers: worker_state.clone(),
                    worker_index: idx,
                    stopping: stopping.clone(),
                },
            )
        })
    }

    fn become_supervisor(&self) {
        let mut workers = self.workers();
        assert!(!workers.iter().any(|worker| worker.is_supervisor()));
        workers[self.worker_index] = WorkerState::Supervisor;
    }

    /// perform state transition after a task has been completed
    /// returns task receiver if this worker goes back to being available, or `None` if it becomes the supervisor
    ///
    /// Panics if worker was not either working or not started
    fn completed_job(
        &self,
        steal: impl Fn(&[Option<T::Key>]) -> Option<T>,
    ) -> PostJobTransition<T> {
        let mut workers = self.workers();
        if let WorkerState::Working(key) = workers[self.worker_index] {
            log::debug!(
                "{}: Task {:?} completed by worker",
                std::thread::current().name().unwrap_or_default(),
                key,
            );
        } else {
            panic!("Worker expected to be working");
        }
        log::debug!(
            "{}: Looking for opportiunity to keep on with {:?}",
            std::thread::current().name().unwrap_or_default(),
            &workers
        );
        let exclusions = workers
            .iter()
            .enumerate()
            .map(|(worker_index, worker)| {
                (worker_index != self.worker_index)
                    .then(|| worker.key())
                    .flatten()
            })
            .collect::<Vec<_>>();
        log::trace!(
            "{}: about to steal",
            std::thread::current().name().unwrap_or_default()
        );
        if let Some(task) = (steal)(&exclusions) {
            log::debug!(
                "{}: started working on {:?} : continuing working unsupervised",
                std::thread::current().name().unwrap_or_default(),
                task.key()
            );
            workers[self.worker_index] = WorkerState::Working(task.key());
            PostJobTransition::KeepWorking(task)
        } else if !workers.iter().any(|worker| worker.is_supervisor()) {
            log::trace!(
                "{}: > No supervisor found, becoming supervisor",
                std::thread::current().name().unwrap_or_default(),
            );
            workers[self.worker_index] = WorkerState::Supervisor;
            PostJobTransition::BecomeSupervisor
        } else if self.stopping.load(atomic::Ordering::Relaxed) {
            log::trace!(
                "{}: > Draining worker has no more to do, stopping",
                std::thread::current().name().unwrap_or_default(),
            );
            workers[self.worker_index] = WorkerState::Stopped;
            PostJobTransition::Stop
        } else {
            let (send, recv) = crossbeam_channel::bounded(1);
            workers[self.worker_index] = WorkerState::Available(send);
            log::trace!(
                "{}: > Supervisor found, becoming available",
                std::thread::current().name().unwrap_or_default(),
            );
            PostJobTransition::BecomeAvailable(recv)
        }
    }

    /// assigns jobs to available workers, changing those workers into the `Working` state.
    /// the first job is assigned to the supervisor itself, the rest are assigned to available workers in order
    ///
    /// panics if this worker is not the supervisor
    fn assign_jobs(&self, steal: &Mutex<dyn Scheduler<T>>) -> Option<T> {
        log::trace!("Assigning jobs");
        let mut workers = self.workers();
        assert!(workers[self.worker_index].is_supervisor());
        let mut working_count = workers.iter().filter(|state| state.is_working()).count();
        let mut exclusions = workers
            .iter()
            .map(|worker| worker.key())
            .collect::<Vec<_>>();

        log::debug!(
            "{}: Supervisor to assign jobs, {} currently working on {:?}",
            std::thread::current().name().unwrap_or_default(),
            working_count,
            workers,
        );
        let available_workers: Vec<_> = workers
            .iter_mut()
            .enumerate()
            .filter(|(_idx, worker)| matches!(worker, WorkerState::Available(_)))
            .collect();
        log::trace!(
            "{}: assign_jobs, about to steal",
            std::thread::current().name().unwrap_or_default()
        );
        let tasks = steal.lock().steal(&exclusions, available_workers.len() + 1);
        log::trace!(
            "{}: assign_jobs, stole {} tasks",
            std::thread::current().name().unwrap_or_default(),
            tasks.len()
        );
        debug_assert!(tasks.len() <= available_workers.len() + 1);
        let mut tasks = tasks.into_iter();
        let mut available_workers = available_workers.into_iter();

        // supervisor self-assigns first task, it's usually faster than waiting for another thread to wake up
        let own_task = tasks.next();

        'tasks: for mut task in tasks {
            let key = task.key();
            for (worker_idx, worker) in &mut available_workers {
                if let WorkerState::Available(send) = worker {
                    log::info!("w{} to schedule {:?}", worker_idx, key);
                    if let Err(SendError(WorkerInstruction::Assign(returned_job))) =
                        send.send(WorkerInstruction::Assign(task))
                    {
                        // if a worker has died, we can try with another
                        task = returned_job;
                        log::warn!("w{} unreachable, task not scheduled", worker_idx);
                    } else {
                        *worker = WorkerState::Working(key);
                        exclusions[worker_idx] = Some(key);
                        working_count += 1;
                        continue 'tasks;
                    }
                } else {
                    unreachable!("Only iterating over available workers");
                }
            }
            log::warn!("no available worker for task, returning to scheduler");
            steal.lock().requeue(task);
        }

        if let Some(task) = own_task {
            log::debug!(
                "w{} started working on {:?} : supervisor becoming worker",
                self.worker_index,
                task.key()
            );
            let available_workers: Vec<_> = available_workers.map(|(idx, _)| idx).collect();
            workers[self.worker_index] = WorkerState::Working(task.key());

            for new_super in available_workers {
                if let WorkerState::Available(send) = &workers[new_super] {
                    if send.send(WorkerInstruction::BecomeSupervisor).is_err() {
                        log::warn!("w{} Failed to assign supervisor", new_super);
                    } else {
                        workers[new_super] = WorkerState::Supervisor;
                        return Some(task);
                    }
                } else {
                    unreachable!()
                }
            }
            log::warn!("Failed to assign any supervisor");
            Some(task)
        } else {
            None
        }
    }

    fn workers(&self) -> MutexGuard<'_, Vec<WorkerState<T>>> {
        self.workers.lock()
    }
}

#[derive(Debug)]
enum PostJobTransition<T> {
    BecomeSupervisor,
    BecomeAvailable(crossbeam_channel::Receiver<WorkerInstruction<T>>),
    KeepWorking(T),
    Stop,
}

/// An instruction from the supervisor to an available worker
#[derive(Debug)]
enum WorkerInstruction<T> {
    Assign(T),
    BecomeSupervisor,
}

enum WorkerState<T: Task> {
    Supervisor,
    Working(T::Key),
    Available(crossbeam_channel::Sender<WorkerInstruction<T>>),
    Stopped,
}

impl<T: Task> fmt::Debug for WorkerState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supervisor => write!(f, "Supervisor"),
            Self::Working(arg0) => f.debug_tuple("Working").field(arg0).finish(),
            Self::Available(arg0) => f.debug_tuple("Available").field(arg0).finish(),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

impl<T: Task> WorkerState<T> {
    fn available() -> (crossbeam_channel::Receiver<WorkerInstruction<T>>, Self) {
        let (send, recv) = crossbeam_channel::bounded(1);
        (recv, Self::Available(send))
    }

    /// if worker is working, returns the key, otherwise `None`
    fn key(&self) -> Option<T::Key> {
        if let Self::Working(key) = self {
            Some(*key)
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
    use std::{collections::VecDeque, mem};

    use super::*;

    #[derive(Debug)]
    struct ExcludedJob(u8);

    impl Task for ExcludedJob {
        type Key = u8;

        fn key(&self) -> Self::Key {
            self.0
        }

        fn execute(self) {}
    }

    struct PrioritisedJob(u8);

    impl Task for PrioritisedJob {
        type Key = ();

        fn key(&self) -> Self::Key {
            ()
        }

        fn execute(self) {}
    }

    /// if a task completes and there is another supervisor, this worker becomes available
    #[test]
    fn working_to_available() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(1),
                WorkerState::Supervisor,
            ])),
            worker_index: 0,
            stopping: Arc::default(),
        };
        let job_recv = state.completed_job(|_| None);
        assert!(matches!(job_recv, PostJobTransition::BecomeAvailable(_)));
        let workers = state.workers.lock();
        assert!(matches!(workers[0], WorkerState::Available(_)));
    }

    /// if a task completes and there is no other supervisor, this worker becomes a supervisor
    #[test]
    fn working_to_supervisor() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(1),
                WorkerState::Working(2),
            ])),
            worker_index: 0,
            stopping: Arc::default(),
        };
        let job_recv = state.completed_job(|_| None);
        assert!(matches!(job_recv, PostJobTransition::BecomeSupervisor));
        let workers = state.workers.lock();
        assert!(workers[0].is_supervisor());
    }

    /// if a task completes and there is another task, this worker remains a worker
    #[test]
    fn working_to_working() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Working(1),
                WorkerState::Working(2),
            ])),
            worker_index: 0,
            stopping: Arc::default(),
        };
        let job_recv = state.completed_job(|_| Some(ExcludedJob(3)));
        assert!(
            matches!(job_recv, PostJobTransition::KeepWorking(ExcludedJob(3))),
            "{:?}",
            job_recv
        );
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
    }

    /// when there is a task to do, the supervisor will do it and pass it's supervisor duty to another
    #[test]
    fn supervisor_assignment() {
        let (send, recv) = crossbeam_channel::unbounded();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Available(send),
            ])),
            worker_index: 0,
            stopping: Arc::default(),
        };
        let scheduler = Mutex::new(TakeScheduler(vec![ExcludedJob(1)].into()));
        assert!(state.assign_jobs(&scheduler).is_some());
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
        assert!(workers[1].is_supervisor());
        assert!(recv.try_recv().is_ok());
    }

    /// when more than one task is assigned an available worker is also given a task
    #[test]
    fn worker_assignment() {
        let (send, recv) = crossbeam_channel::unbounded();
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Available(send),
            ])),
            worker_index: 0,
            stopping: Arc::default(),
        };
        let scheduler = Mutex::new(TakeScheduler(vec![ExcludedJob(1), ExcludedJob(2)].into()));
        assert!(state.assign_jobs(&scheduler).is_some());
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
        assert!(workers[1].is_working());
        assert!(recv.try_recv().is_ok());
    }

    /// a supervisor stops supervising and switch to working, no worker is available to become supervisor
    #[test]
    fn supervisor_to_working() {
        let state = RunnerState::<ExcludedJob> {
            workers: Arc::new(Mutex::new(vec![
                WorkerState::Supervisor,
                WorkerState::Working(1),
            ])),
            worker_index: 0,
            stopping: Arc::default(),
        };
        let scheduler = Mutex::new(TakeScheduler(vec![ExcludedJob(2)].into()));
        assert!(state.assign_jobs(&scheduler).is_some());
        let workers = state.workers.lock();
        assert!(workers[0].is_working());
        assert!(workers[1].is_working());
    }

    struct TakeScheduler<T>(VecDeque<T>);

    impl<T: Task> Scheduler<T> for TakeScheduler<T> {
        fn steal(&mut self, _running: &[Option<T::Key>], limit: usize) -> Vec<T> {
            if limit as usize >= self.0.len() {
                mem::take(&mut self.0).into()
            } else {
                let remaining = self.0.split_off(limit as usize);
                mem::replace(&mut self.0, remaining).into()
            }
        }

        fn requeue(&mut self, task: T) {
            self.0.push_front(task)
        }
    }
}

#[cfg(test)]
mod runner_test {
    use std::{thread, time::Duration};

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum Event<T> {
        Start(T),
        End(T),
    }

    #[derive(Debug)]
    struct ExcludedJob(u8, Arc<Mutex<Vec<Event<u8>>>>);

    impl Task for ExcludedJob {
        type Key = u8;

        fn key(&self) -> Self::Key {
            self.0
        }

        fn execute(self) {
            log::info!("Executing #{}", self.key());
            self.1.lock().push(Event::Start(self.0));
            thread::sleep(Duration::from_millis(2));
            self.1.lock().push(Event::End(self.0));
            log::info!("Executed #{}", self.key());
        }
    }

    struct Super {
        load_count: u8,
        events: Arc<Mutex<Vec<Event<u8>>>>,
    }

    impl Scheduler<ExcludedJob> for Super {
        fn steal(&mut self, running: &[Option<u8>], limit: usize) -> Vec<ExcludedJob> {
            let load_num = self.load_count;
            self.load_count += 1;
            log::info!("steal number {}, running {:?}", load_num, running);
            match load_num {
                0 => {
                    assert_eq!(limit, 2);
                    assert_eq!(running.iter().flatten().count(), 0);
                    vec![ExcludedJob(1, self.events.clone())]
                }
                1 => {
                    assert_eq!(limit, 1);
                    assert_eq!(running.iter().flatten().next(), Some(&1));
                    vec![ExcludedJob(2, self.events.clone())]
                }
                2 => {
                    assert_eq!(limit, 1);
                    assert_eq!(running.iter().flatten().next(), Some(&2));
                    vec![]
                }
                3 => {
                    assert_eq!(limit, 1);
                    assert_eq!(running.iter().flatten().next(), None);
                    vec![]
                }
                4 => {
                    // assert_eq!(limit, 1); // FIXME in this test, this is sometimes 1 and sometimes 2, meaning sometimes the supervisor gets there first and sometimes the worker, I'm not sure which one it should be or if it even matters but I would liek the test to be deterministic
                    assert_eq!(running.iter().flatten().next(), None);
                    vec![ExcludedJob(4, self.events.clone())]
                }
                _ => vec![],
            }
        }

        fn requeue(&mut self, _task: ExcludedJob) {
            todo!()
        }
    }

    struct Load;
    impl Loader<ExcludedJob> for Load {
        type Scheduler = Super;

        fn load(&mut self, _idle: bool, _scheduler: &Mutex<Super>) {
            thread::sleep(Duration::from_millis(1))
        }
    }

    #[test]
    fn working_to_supervisor_excluded() {
        simple_logger::SimpleLogger::new().init().unwrap();

        let events = Arc::new(Mutex::new(vec![]));

        let supervisor = Super {
            load_count: 0,
            events: events.clone(),
        };

        let pool = spawn(2, supervisor, Load, false);

        thread::sleep(Duration::from_millis(100));
        pool.stop();
        assert_eq!(
            *events.lock(),
            vec![
                Event::Start(1),
                Event::Start(2),
                Event::End(1),
                Event::End(2),
                Event::Start(4),
                Event::End(4) // FIXME sometimes 4 does not run
            ]
        );
    }
}
