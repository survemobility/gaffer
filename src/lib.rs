//! Prioritised, parallel job scheduler with concurrent exclusion, job merging, recurring jobs and load limiting for lower priorities.
//!
//! A job scheduler executes tasks on it's own thread or thread pool. This job scheduler is particularly designed to consider heavier weight or more expensive jobs, which likely have side effects. In this case it can be valuable to prioritise the jobs and merge alike jobs in the queue.
//!
//! __Features__
//!
//! * Recurring jobs: jobs which will be re-enqueued at some interval <sup>2</sup>
//! * Job queue: use an [`crossbeam_channel::Sender<YourJob>`] to send jobs
//! * Future Jobs: (Optionally) create `Future`s to get results from the jobs <sup>2</sup>
//! * Job prioritisation: provide a priority for jobs and all the jobs will be executed in that order
//! * Job merging: merge identical / similar jobs in the queue to reduce workload <sup>2</sup>
//! * Parallel execution: run jobs on multiple threads and lock jobs which should be run exclusively, they remain in the queue and don't occupy other resources
//! * Priority throttling: in order to have idle threads ready to pick up higher-priority jobs, throttle lower priority jobs by restricting them to a lower number of threads
//!
//! __Limitations__
//!
//! * <sup>2</sup> There are a few ergonomics issues to do with the job merging and recurring jobs apis. For example, all jobs need to implement [`Clone`] (so they can be reproduced for recurring) and any results provided by a future need to implement `Clone` (so that they can be merged).
//! * some of the tests are very dependent on timing and will fail if run slowly
//!
//!
//! ## Example
//!
//! [See `/examples/example.rs`](./examples/example.rs)
//!
//! ```rust
//! use gaffer::*;
//! use std::{thread, time::Duration};
//!
//! #[derive(Debug)]
//! struct WaitJob(Duration, u8, Option<char>);
//!
//! impl Job for WaitJob {
//!     type Exclusion = ExclusionOption<char>;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         self.2.into()
//!     }
//!
//!     fn execute(self) {
//!         thread::sleep(self.0);
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! impl Prioritised for WaitJob {
//!     type Priority = u8;
//!
//!     fn priority(&self) -> Self::Priority {
//!         self.1.into()
//!     }
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> =
//!         Some(|me: Self, other: &mut Self| -> MergeResult<Self> {
//!             if me.1 == other.1 {
//!                 other.0 += me.0;
//!                 other.1 = other.1.max(me.1);
//!                 MergeResult::Success
//!             } else {
//!                 MergeResult::NotMerged(me)
//!             }
//!         });
//! }
//! ```
//!
//! ## Capabilities
//!
//! ### Recurring jobs
//! ### Job queue
//! ### Future jobs
//! ### Job prioritisation
//! ### Job merging
//! ### Parallel execution
//! ### Priority throttling
//!
//! ## Usage
//!
//! ```toml
//! [dependencies]
//! gaffer = { git = "ssh://git@github.com/survemobility/gaffer.git", branch = "pr-1" }
//! ```

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

use parking_lot::Mutex;

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use runner::ConcurrencyLimitFn;
use source::{IntervalRecurringJob, NeverRecur, RecurringJob, SourceManager};

pub mod future;
mod runner;
mod source;

/// Top level structure of the crate. Currently, recirring jobs would keep being scheduled once this is dropped, but that will probably change.
///
/// See crate level docs
pub struct JobRunner<J: Prioritised + 'static> {
    sender: crossbeam_channel::Sender<J>,
}

impl<J: Job + 'static> JobRunner<J> {
    /// Create a Builder to start building a [`JobRunner`]
    pub fn builder() -> Builder<J, NeverRecur> {
        Builder {
            concurrency_limit: Box::new(|_: <J as Prioritised>::Priority| None as Option<u8>),
            recurring: vec![],
        }
    }

    /// Send a job to the queue
    pub fn send(&self, job: J) -> Result<(), crossbeam_channel::SendError<J>> {
        self.sender.send(job)
    }
}

impl<J: Prioritised + 'static> Clone for JobRunner<J> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

/// Builder of [`JobRunner`]
pub struct Builder<J: Job + 'static, R> {
    concurrency_limit: Box<ConcurrencyLimitFn<J>>,
    recurring: Vec<R>,
}

impl<J: Job + Send + Clone + 'static> Builder<J, IntervalRecurringJob<J>> {
    /// Start building a [`JobRunner`]
    pub fn new() -> Self {
        Builder {
            concurrency_limit: Box::new(|_: <J as Prioritised>::Priority| None as Option<u8>),
            recurring: vec![],
        }
    }

    /// Set a job as recurring, the job will be executed every time `interval` passes since the last execution of a matching job
    pub fn set_recurring(mut self, interval: Duration, last_enqueue: Instant, job: J) -> Self {
        self.recurring.push(IntervalRecurringJob {
            last_enqueue,
            interval,
            job,
        });
        self
    }
}

impl<J: Job + Send + Clone + 'static> Default for Builder<J, source::IntervalRecurringJob<J>> {
    fn default() -> Self {
        Self::new()
    }
}

impl<J: Job + 'static, R: RecurringJob<J> + Send + 'static> Builder<J, R> {
    /// Function determining, for each priority, how many threads can be allocated to jobs of this priority, any remaining threads will be left idle to service higher-priority jobs. `None` means parallelism won't be limited
    pub fn limit_concurrency(
        mut self,
        concurrency_limit: impl Fn(<J as Prioritised>::Priority) -> Option<u8> + Send + Sync + 'static,
    ) -> Self {
        self.concurrency_limit = Box::new(concurrency_limit);
        self
    }

    /// Build the [`JobRunner`], spawning `thread_num` threads as workers
    pub fn build(self, thread_num: usize) -> JobRunner<J> {
        let (sender, sources) = SourceManager::<J, R>::new_with_recurring(self.recurring);
        let jobs = Arc::new(Mutex::new(sources));
        let _threads = runner::spawn(thread_num, jobs, self.concurrency_limit);
        JobRunner { sender }
    }
}

/// A job which can be executed by the runner, with features to synchronise jobs that would interfere with each other and reduce the parallelisation of low priority jobs
pub trait Job: Prioritised + Send {
    /// Type used to check which jobs should not be allowed to run concurrently, see [`Job::exclusion()`]. Use [`NoExclusion`] for jobs which can always be run at the same time, see also [`ExclusionOption`].
    type Exclusion: PartialEq + Copy + fmt::Debug + Send;

    /// Used to check which jobs should not be allowed to run concurrently, if `<Job::Exclusion as PartialEq>::eq(job1.exclusion(), job2.exclusion())`, then `job1` and `job2` can't run at the same time.
    fn exclusion(&self) -> Self::Exclusion;

    /// Execute and consume the job
    fn execute(self);
}

/// A type that can be put in a priority queue, tells the queue which order the items should come out in, whether / how to merge them, and checking whether item's match
pub trait Prioritised: Sized {
    /// Type of the priority, the higher prioritys are those which are larger based on [`Ord::cmp`].
    type Priority: Ord + Copy + Send;

    /// Get the priority of this thing
    fn priority(&self) -> Self::Priority;

    /// not part of prioritisation, but allows that recurring jobs can be reset when a matching job is added to the queue
    fn matches(&self, _job: &Self) -> bool {
        false
    }

    /// optional function to allow merging of jobs
    const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;
}

/// Result of an attempted merge, see [`Prioritised::ATTEMPT_MERGE_INTO`]
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
    /// This job excludes all others, it can only be run whilst all other workers are idle. NOTE: If the runner is busy this will have to wait until all jobs are finished
    All,
    /// This job excludes some other jobs which match `T`
    Some(T),
    /// This job excludes no other jobs and can run at any time
    None,
}

impl<T: PartialEq> PartialEq for ExclusionOption<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ExclusionOption::Some(me), ExclusionOption::Some(other)) => me == other,
            (ExclusionOption::All, _) => true,
            (_, ExclusionOption::All) => true,
            _ => false,
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
