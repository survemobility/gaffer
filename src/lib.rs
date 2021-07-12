///! # chief
///!
///! Prioritised, parallel job scheduler with concurrent exclusion, job merging, recurring jobs and load limiting for lower priorities.
///!
///! A job scheduler executes tasks on it's own thread or thread pool. This job scheduler is particularly designed to consider heavier weight or more expensive jobs, which likely have side effects. In this case it can be valuable to prioritise the jobs and merge alike jobs in the queue.
///!
///! __Features__
///!
///! * Recurring jobs: jobs which will be re-enqueued at some interval <sup>2</sup>
///! * Job queue: use an `crossbeam_channel::Sender<YourJob>` to send jobs
///! * Future Jobs: (Optionally) create `Future`s to get results from the jobs <sup>2</sup>
///! * Job prioritisation: provide a priority for jobs and all the jobs will be executed in that order
///! * Job merging: merge identical / similar jobs in the queue to reduce workload <sup>2</sup>
///! * Parallel execution: run jobs on multiple threads and lock jobs which should be run exclusively, they remain in the queue and don't occupy other resources
///! * Priority throttling: in order to have idle threads ready to pick up higher-priority jobs, throttle lower priority jobs by restricting them to a lower number of threads
///!
///! __Limitations__
///!
///! * <sup>2</sup> There are a few ergonomics issues to do with the job merging and recurring jobs apis. For example, all jobs need to implement `Clone` (so they can be reproduced for recurring) and any results provided by a future need to implement `Clone` (so that they can be merged).
///! * some of the tests are very dependent on timing and will fail if run slowly
///!
///!
///! ## Example
///!
///! [See `/examples/example.rs`](./examples/example.rs)
///!
///! ```rust
///! #[derive(Debug)]
///! struct WaitJob(Duration, u8, Option<char>);
///!
///! impl Job for WaitJob {
///!     type Exclusion = ExclusionOption<char>;
///!
///!     fn exclusion(&self) -> Self::Exclusion {
///!         self.2.into()
///!     }
///!
///!     fn execute(self) {
///!         thread::sleep(self.0);
///!         println!("Completed job {:?}", self);
///!     }
///! }
///!
///! impl Prioritised for WaitJob {
///!     type Priority = u8;
///!
///!     fn priority(&self) -> Self::Priority {
///!         self.1.into()
///!     }
///!
///!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> =
///!         Some(|me: Self, other: &mut Self| -> MergeResult<Self> {
///!             if me.1 == other.1 {
///!                 other.0 += me.0;
///!                 other.1 = other.1.max(me.1);
///!                 MergeResult::Success
///!             } else {
///!                 MergeResult::NotMerged(me)
///!             }
///!         });
///! }
///! ```
///!
///! ## Capabilities
///!
///! ### Recurring jobs
///! ### Job queue
///! ### Future jobs
///! ### Job prioritisation
///! ### Job merging
///! ### Parallel execution
///! ### Priority throttling
///!
///! ## Usage
///!
///! ```toml
///! [dependencies]
///! chief = { git = "ssh://git@github.com/survemobility/chief.git", branch = "pr-1" }
///! ```
use parking_lot::Mutex;

use std::{
    fmt,
    panic::UnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};

use runner::ConcurrencyLimitFn;
use source::{IntervalRecurringJob, RecurringJob, SourceManager};

pub mod future;
mod runner;
pub mod source;

pub struct JobRunner<J: Prioritised + 'static, R = IntervalRecurringJob<J>> {
    sender: crossbeam_channel::Sender<J>,
    jobs: Arc<Mutex<SourceManager<J, R>>>,
}

impl<J: Job + 'static, R: RecurringJob<J>> JobRunner<J, R> {
    pub fn builder() -> Builder<J, R> {
        Builder {
            concurrency_limit: Box::new(|_: <J as Prioritised>::Priority| None as Option<u8>),
            recurring: vec![],
        }
    }

    pub fn send(&self, job: J) -> Result<(), crossbeam_channel::SendError<J>> {
        self.sender.send(job)
    }
}

impl<J: Prioritised + 'static, R> Clone for JobRunner<J, R> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            jobs: self.jobs.clone(),
        }
    }
}

pub struct Builder<J: Job + 'static, R> {
    concurrency_limit: Box<ConcurrencyLimitFn<J>>,
    recurring: Vec<R>,
}

impl<J: Job + Send + Clone + 'static> Builder<J, IntervalRecurringJob<J>> {
    /// Set a job as recurring, the job will be executed every time `interval` passes since the last execution of a matching job
    pub fn set_recurring(&mut self, interval: Duration, last_enqueue: Instant, job: J) {
        self.recurring.push(IntervalRecurringJob {
            last_enqueue,
            interval,
            job,
        });
    }
}

impl<J: Job + UnwindSafe + 'static, R: RecurringJob<J> + Send + 'static> Builder<J, R> {
    /// Function determining, for each priority, how many threads can be allocated to jobs of this priority, any remaining threads will be left idle to service higher-priority jobs. `None` means parallelism won't be limited
    pub fn limit_concurrency(
        mut self,
        concurrency_limit: impl Fn(<J as Prioritised>::Priority) -> Option<u8> + Send + Sync + 'static,
    ) -> Self {
        self.concurrency_limit = Box::new(concurrency_limit);
        self
    }

    pub fn build(self, thread_num: usize) -> JobRunner<J, R> {
        let (sender, sources) = SourceManager::<J, R>::new_with_recurring(self.recurring);
        let jobs = Arc::new(Mutex::new(sources));
        let _threads = runner::spawn(thread_num, jobs.clone(), self.concurrency_limit);
        JobRunner { sender, jobs }
    }
}

/// A job which can be executed by the runner, with features to synchronise jobs that would interfere with each other and reduce the parallelisation of low priority jobs
pub trait Job: Prioritised + Send {
    type Exclusion: PartialEq + Copy + fmt::Debug + Send;

    /// exclude jobs which can't be run concurrently. if .`exclusion()` matches for 2 jobs, the runner won't run them at the same time
    fn exclusion(&self) -> Self::Exclusion;

    fn execute(self);
}

pub trait Prioritised: Sized {
    type Priority: Ord + Copy + Send;

    fn priority(&self) -> Self::Priority;

    fn matches(&self, _job: &Self) -> bool {
        false
    }

    /// optional function to allow merging of jobs
    const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;
}

pub enum MergeResult<P> {
    /// merge was sucessful, eg. either because the items are the same or one is a superset of the other
    Success,
    /// the attempted items were not suitable for merging
    NotMerged(P),
}

/// Allows any jobs to run at the same time
#[derive(Debug, Copy, Clone)]
pub struct NoExclusion;

impl PartialEq for NoExclusion {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

/// Allows some jobs to be run at the same time, others to acquire a keyed exclusive lock, and others to acquire a global exclusive lock
#[derive(Debug, Copy, Clone)]
pub enum ExclusionOption<T> {
    All,
    Some(T),
    None,
}

impl<T: PartialEq> PartialEq for ExclusionOption<T> {
    fn eq(&self, other: &Self) -> bool {
        if let (ExclusionOption::Some(me), ExclusionOption::Some(other)) = (self, other) {
            me == other
        } else {
            false
        }
    }
}

impl<T> From<Option<T>> for ExclusionOption<T> {
    fn from(val: Option<T>) -> Self {
        if let Some(val) = val {
            ExclusionOption::Some(val)
        } else {
            ExclusionOption::None
        }
    }
}

impl<T> From<T> for ExclusionOption<T> {
    fn from(val: T) -> Self {
        ExclusionOption::Some(val)
    }
}
