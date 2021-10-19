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
//! [See `/examples/full.rs`](./examples/full.rs) for a full example, or below for examples focusing on particular capabilities.
//!
//! ## Capabilities
//!
//! The capabilities can all be combined, see the full example.
//!
//! ### Recurring jobs
//!
//! Recurring jobs are configured on the runner when it is built, the runner then clones the job and enquese it if a matching job is not enqueued within the interval.
//!
//! You need to call [`Builder::set_recurring`] and you need to implement [`Prioritised::matches`].
//!
//! ```
//! use std::time::Duration;
//! use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let _runner = Builder::new()
//!         .set_recurring(
//!             Duration::from_secs(2),
//!             std::time::Instant::now(),
//!             MyJob("recurring"),
//!         )
//!         .build(1);
//!
//!     std::thread::sleep(Duration::from_secs(7));
//!     Ok(())
//! }
//!
//! #[derive(Debug, Clone)]
//! struct MyJob(&'static str);
//!
//! impl Job for MyJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     fn execute(self) {
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! /// This Job isn't actually prioritised but this trait needs to be implemented for now
//! impl Prioritised for MyJob {
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;
//!
//!     /// matches needs to be implemented for recurring jobs, it must return `true` for a `.clone()` of it's self
//!     fn matches(&self, job: &Self) -> bool {
//!         self.0 == job.0
//!     }
//! }
//! ```
//!
//! ### Job queue
//!
//! Call [`JobRunner::send`] to add jobs onto the queue, they will be executed in the order that they are enqueued
//!
//! ```
//! use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = Builder::new().build(1);
//!
//!     for i in 1..=5 {
//!         runner.send(WaitJob(format!("Job {}", i)))?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(Duration::from_secs(7));
//!     Ok(())
//! }
//!
//! #[derive(Debug, Clone)]
//! struct WaitJob(String);
//!
//! impl Job for WaitJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! /// This Job isn't actually prioritised but this trait needs to be implemented for now
//! impl Prioritised for WaitJob {
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;
//!
//!     fn matches(&self, _job: &Self) -> bool {
//!         false
//!     }
//! }
//! ```
//!
//! ### Job prioritisation
//!
//! Return a value from [`Prioritised::priority`] and jobs from the queue will be executed in priority order
//!
//! ```
//! use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = Builder::new().build(1);
//!
//!     for (i, priority) in (1..=5).zip([1, 2].iter().cycle()) {
//!         runner.send(PrioritisedJob(format!("Job {}", i), *priority))?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(Duration::from_secs(7));
//!     Ok(())
//! }
//!
//! #[derive(Debug, Clone)]
//! struct PrioritisedJob(String, u8);
//!
//! impl Job for PrioritisedJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! impl Prioritised for PrioritisedJob {
//!     type Priority = u8;
//!
//!     /// This Job is prioritied
//!     fn priority(&self) -> Self::Priority {
//!         self.1
//!     }
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;
//!
//!     fn matches(&self, _job: &Self) -> bool {
//!         false
//!     }
//! }
//! ```
//!
//! ### Job merging
//!
//! Gracefully handle spikes in duplicate or overlapping jobs by automatically merging those jobs in the queue.
//! Implement the [`Prioritised::ATTEMPT_MERGE_INTO`] function.
//!
//! ```
//! use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = Builder::new().build(1);
//!
//!     for i in 10..=50 {
//!         runner.send(MergeJob(format!("Job {}", i)))?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(Duration::from_secs(7));
//!     Ok(())
//! }
//!
//! #[derive(Debug, Clone)]
//! struct MergeJob(String);
//!
//! impl Job for MergeJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! /// This Job isn't actually prioritised but this trait implements the job merge
//! impl Prioritised for MergeJob {
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> =
//!         Some(|this, that| {
//!             if this.matches(that) {
//!                 that.0 = format!("{}x", &that.0[..that.0.len() - 1]);
//!                 MergeResult::Success
//!             } else {
//!                 MergeResult::NotMerged(this)
//!             }
//!         });
//!
//!     fn matches(&self, that: &Self) -> bool {
//!         self.0[..self.0.len() - 1] == that.0[..that.0.len() - 1]
//!     }
//! }
//! ```
//!
//! ### Parallel execution
//!
//! Jobs can be run over multiple threads, just provide the number of threads to [`Builder::build`]
//!
//! ```
//! use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = Builder::new().build(10);
//!
//!     for i in 1..=50 {
//!         runner.send(WaitJob(format!("Job {}", i)))?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(Duration::from_secs(7));
//!     Ok(())
//! }
//!
//! #[derive(Debug, Clone)]
//! struct WaitJob(String);
//!
//! impl Job for WaitJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! /// This Job isn't actually prioritised but this trait needs to be implemented for now
//! impl Prioritised for WaitJob {
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;
//!
//!     fn matches(&self, _job: &Self) -> bool {
//!         false
//!     }
//! }
//! ```
//!
//! ### Priority throttling
//!
//! Lower priority jobs can be restricted to less threads to reduce the load on system resources and encourage merging (if using).
//!
//! Use [`Builder::limit_concurrency`].
//!
//! ```
//! use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = Builder::new()
//!         .limit_concurrency(|priority| (priority == 1).then(|| 1))
//!         .build(4);
//!
//!     for (i, priority) in (1..=10).zip([1, 2].iter().cycle()) {
//!         runner.send(PrioritisedJob(format!("Job {}", i), *priority))?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(Duration::from_secs(7));
//!     Ok(())
//! }
//!
//! #[derive(Debug, Clone)]
//! struct PrioritisedJob(String, u8);
//!
//! impl Job for PrioritisedJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! impl Prioritised for PrioritisedJob {
//!     type Priority = u8;
//!
//!     /// This Job is prioritied
//!     fn priority(&self) -> Self::Priority {
//!         self.1
//!     }
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;
//!
//!     fn matches(&self, _job: &Self) -> bool {
//!         false
//!     }
//! }
//! ```
//!
//! ### Future jobs
//!
//! Use a [`Promise`] in the job to allow `await`ing job results in async code. When combined with merging, all the futures of the merged jobs will complete with clones of the single job which actually ran
//!
//! ```
//! use gaffer::{
//!     future::{Promise, PromiseFuture},
//!     Builder, Job, JobRunner, MergeResult, NoExclusion, Prioritised,
//! };
//! use std::time::Duration;
//!
//! use futures::{executor::block_on, FutureExt, StreamExt};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = Builder::new().build(1);
//!
//!     let mut futures: futures::stream::SelectAll<_> = (10..=50)
//!         .filter_map(|i| {
//!             ProcessString::new(format!("Job {}", i), &runner)
//!                 .ok()
//!                 .map(|f| f.into_stream())
//!         })
//!         .collect();
//!     println!("Jobs enqueued");
//!
//!     block_on(async {
//!         while let Some(result) = futures.next().await {
//!             let processed_string = result.unwrap();
//!             println!(">> {}", processed_string);
//!         }
//!     });
//!     Ok(())
//! }
//!
//! struct ProcessString(String, Promise<String>);
//!
//! impl ProcessString {
//!     fn new(
//!         name: String,
//!         runner: &JobRunner<ProcessString>,
//!     ) -> Result<PromiseFuture<String>, crossbeam_channel::SendError<ProcessString>> {
//!         let (promise, future) = Promise::new();
//!         runner.send(ProcessString(name, promise))?;
//!         Ok(future)
//!     }
//! }
//!
//! /// Clone is needed for recurring jobs which doesn't make sense for promises, it is a deficiency in the api that Clone needs to be implemented here and it won't be called
//! impl Clone for ProcessString {
//!     fn clone(&self) -> Self {
//!         panic!()
//!     }
//! }
//!
//! impl Job for ProcessString {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     fn execute(self) {
//!         println!("Processing job {}", self.0);
//!         std::thread::sleep(Duration::from_secs(1));
//!         self.1.fulfill(format!("Processed : [{}]", self.0));
//!     }
//! }
//!
//! /// This Job isn't actually prioritised but this trait needs to be implemented for now
//! impl Prioritised for ProcessString {
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> =
//!         Some(|this, that| {
//!             if this.matches(that) {
//!                 that.0 = format!("{}x", &that.0[..that.0.len() - 1]);
//!                 that.1.merge(this.1);
//!                 MergeResult::Success
//!             } else {
//!                 MergeResult::NotMerged(this)
//!             }
//!         });
//!
//!     fn matches(&self, that: &Self) -> bool {
//!         self.0[..self.0.len() - 1] == that.0[..that.0.len() - 1]
//!     }
//! }
//! ```
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

    /// Set a job as recurring, the job will be enqueued every time `interval` passes since the `last_enqueue` of a matching job
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
