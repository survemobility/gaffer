//! Prioritised, parallel job scheduler with concurrent exclusion, job merging, recurring jobs and load limiting for lower priorities.
//!
//! A job scheduler executes tasks on it's own thread or thread pool. This job scheduler is particularly designed to consider heavier weight or more expensive jobs, which likely have side effects. In this case it can be valuable to prioritise the jobs and merge alike jobs in the queue.
//!
//! __Features__
//!
//! * Recurring jobs: jobs which will be re-enqueued at some interval
//! * Job queue: send jobs from various threads using the cloneable [`JobRunner`]
//! * Future Jobs: (Optionally) create `Future`s to get results from the jobs
//! * Job prioritisation: provide a priority for jobs and all the jobs will be executed in that order
//! * Job merging: merge identical / similar jobs in the queue to reduce workload
//! * Parallel execution: run jobs on multiple threads and lock jobs which should be run exclusively, they remain in the queue and don't occupy other resources
//! * Concurrent exclusion: key-based locking to avoid jobs running concurrently which shouldn't
//! * Priority throttling: in order to have idle threads ready to pick up higher-priority jobs, throttle lower priority jobs by restricting them to a lower number of threads
//!
//! __Limitations__
//!
//! * some of the tests are very dependent on timing and will fail if run slowly
//!
//! ## Example
//!
//! See `/examples/full.rs` for a full example, or below for examples focusing on particular capabilities.
//!
//! ## Capabilities
//!
//! Below, the examples show minimal usages of each of the capabilities for clarity, but if you're using this you probably want most or all of these.
//!
//! ### Recurring jobs
//!
//! Recurring jobs are configured on the runner when it is built, the runner then clones the job and enquese it if a matching job is not enqueued within the interval.
//!
//! You need to call [`Builder::set_recurring`] and you need to implement [`RecurrableJob`].
//!
//! ```
//! use gaffer::{Job, JobRunner, NoExclusion, RecurrableJob};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let _runner = JobRunner::builder()
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
//!
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//! }
//!
//! impl RecurrableJob for MyJob {
//!     fn matches(&self, other: &Self) -> bool {
//!         self.0 == other.0
//!     }
//! }
//!
//! ```
//!
//! ### Job queue
//!
//! Call [`JobRunner::send`] to add jobs onto the queue, they will be executed in the order that they are enqueued
//!
//! ```
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = gaffer::JobRunner::builder().build(1);
//!
//!     for i in 1..=5 {
//!         let name = format!("Job {}", i);
//!         runner.send(move || {
//!             std::thread::sleep(std::time::Duration::from_secs(1));
//!             println!("Completed job {:?}", name);
//!         })?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(std::time::Duration::from_secs(7));
//!     Ok(())
//! }
//! ```
//!
//! ### Job prioritisation
//!
//! Return a value from [`Job::priority`] and jobs from the queue will be executed in priority order
//!
//! ```
//! use gaffer::{Job, JobRunner, NoExclusion};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = JobRunner::builder().build(1);
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
//! #[derive(Debug)]
//! struct PrioritisedJob(String, u8);
//!
//! impl Job for PrioritisedJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     type Priority = u8;
//!
//!     /// This Job is prioritied
//!     fn priority(&self) -> Self::Priority {
//!         self.1
//!     }
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! ```
//!
//! ### Job merging
//!
//! Gracefully handle spikes in duplicate or overlapping jobs by automatically merging those jobs in the queue.
//! Call [`Builder::enable_merge`].
//!
//! ```
//! use gaffer::{Job, JobRunner, MergeResult, NoExclusion};
//! use std::{sync::{Arc, atomic::{AtomicU8, Ordering}}, time::Duration};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = JobRunner::builder().enable_merge(merge_jobs).build(1);
//!    let counter = Arc::new(AtomicU8::new(0));
//!
//!     for i in 10..=50 {
//!         runner.send(MergeJob(format!("Job {}", i), counter.clone()))?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(Duration::from_secs(7));
//!     assert_eq!(counter.load(Ordering::SeqCst), 5);
//!     Ok(())
//! }
//!
//! #[derive(Debug)]
//! struct MergeJob(String, Arc<AtomicU8>);
//!
//! impl Job for MergeJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!         self.1.fetch_add(1, Ordering::SeqCst);
//!     }
//! }
//!
//! fn merge_jobs(this: MergeJob, that: &mut MergeJob) -> MergeResult<MergeJob> {
//!     if this.0[..this.0.len() - 1] == that.0[..that.0.len() - 1] {
//!         that.0 = format!("{}x", &that.0[..that.0.len() - 1]);
//!         MergeResult::Success
//!     } else {
//!         MergeResult::NotMerged(this)
//!     }
//! }
//!
//! ```
//!
//! ### Parallel execution
//!
//! Jobs can be run over multiple threads, just provide the number of threads to [`Builder::build`]
//!
//! ```
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = gaffer::JobRunner::builder().build(10);
//!
//!     for i in 1..=50 {
//!         let name = format!("WaitJob {}", i);
//!         runner.send(move || {
//!             std::thread::sleep(std::time::Duration::from_secs(1));
//!             println!("Completed job {:?}", name);
//!         })?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(std::time::Duration::from_secs(7));
//!     Ok(())
//! }
//! ```
//!
//! ### Concurrent exclusion
//!
//! Exclusion keys can be provided to show which jobs need to be run exclusively
//!
//! ```
//! use gaffer::{ExclusionOption, Job, JobRunner};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = JobRunner::builder().build(2);
//!
//!     for (i, exclusion) in (1..=10).zip([ExclusionOption::Some(1), ExclusionOption::Some(2), ExclusionOption::None].iter().cycle()) {
//!         runner.send(ExcludedJob(format!("Job {}", i), *exclusion))?;
//!     }
//!
//!     println!("Jobs enqueued");
//!     std::thread::sleep(Duration::from_secs(7));
//!     Ok(())
//! }
//!
//! #[derive(Debug)]
//! struct ExcludedJob(String, ExclusionOption<u8>);
//!
//! impl Job for ExcludedJob {
//!     type Exclusion = ExclusionOption<u8>;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         self.1
//!     }
//!
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! ```
//!
//! ### Priority throttling
//!
//! Lower priority jobs can be restricted to less threads to reduce the load on system resources and encourage merging (if using).
//!
//! Use [`Builder::limit_concurrency`].
//!
//! ```
//! use gaffer::{Job, JobRunner, NoExclusion};
//! use std::time::Duration;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = JobRunner::builder()
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
//! #[derive(Debug)]
//! struct PrioritisedJob(String, u8);
//!
//! impl Job for PrioritisedJob {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     type Priority = u8;
//!
//!     /// This Job is prioritied
//!     fn priority(&self) -> Self::Priority {
//!         self.1
//!     }
//!
//!     fn execute(self) {
//!         std::thread::sleep(Duration::from_secs(1));
//!         println!("Completed job {:?}", self);
//!     }
//! }
//!
//! ```
//!
//! ### Future jobs
//!
//! Use a [`future::Promise`] in the job to allow `await`ing job results in async code. When combined with merging, all the futures of the merged jobs will complete with clones of the single job which actually ran
//!
//! ```
//! use gaffer::{
//!     future::{Promise, PromiseFuture},
//!     Job, JobRunner, MergeResult, NoExclusion,
//! };
//! use std::time::Duration;
//!
//! use futures::{executor::block_on, FutureExt, StreamExt};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runner = JobRunner::builder()
//!         .enable_merge(|this: ProcessString, that: &mut ProcessString| {
//!             if this.0[..this.0.len() - 1] == that.0[..that.0.len() - 1] {
//!                 that.0 = format!("{}x", &that.0[..that.0.len() - 1]);
//!                 that.1.merge(this.1);
//!                 MergeResult::Success
//!             } else {
//!                 MergeResult::NotMerged(this)
//!             }
//!         })
//!         .build(1);
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
//! impl Job for ProcessString {
//!     type Exclusion = NoExclusion;
//!
//!     fn exclusion(&self) -> Self::Exclusion {
//!         NoExclusion
//!     }
//!
//!     type Priority = ();
//!
//!     fn priority(&self) -> Self::Priority {}
//!
//!     fn execute(self) {
//!         println!("Processing job {}", self.0);
//!         std::thread::sleep(Duration::from_secs(1));
//!         self.1.fulfill(format!("Processed : [{}]", self.0));
//!     }
//! }
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
pub use source::RecurrableJob;
use source::{IntervalRecurringJob, RecurringJob, SourceManager};

pub mod future;
mod runner;
mod source;

/// Top level structure of the crate. Currently, recurring jobs would keep being scheduled once this is dropped, but that will probably change.
///
/// See crate level docs
pub struct JobRunner<J> {
    sender: crossbeam_channel::Sender<J>,
}

impl<J: Job + 'static> JobRunner<J> {
    /// Create a Builder to start building a [`JobRunner`]
    pub fn builder() -> Builder<J> {
        Builder::new()
    }

    /// Send a job to the queue
    pub fn send(&self, job: J) -> Result<(), crossbeam_channel::SendError<J>> {
        self.sender.send(job)
    }
}

impl<J> Clone for JobRunner<J> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

/// Builder of [`JobRunner`]
pub struct Builder<J: Job + 'static> {
    concurrency_limit: Box<ConcurrencyLimitFn<J>>,
    recurring: Vec<Box<dyn RecurringJob<J> + Send>>,
    /// optional function to allow merging of jobs
    merge_fn: Option<fn(J, &mut J) -> MergeResult<J>>,
}

impl<J: Job + Send + 'static> Builder<J> {
    /// Start building a [`JobRunner`]
    fn new() -> Self {
        Builder {
            concurrency_limit: Box::new(|_: <J as Job>::Priority| None as Option<u8>),
            recurring: vec![],
            merge_fn: None,
        }
    }

    /// Enable merging of Jobs in the queue, if a merge function is provided here, it will be tried with each job added to the queue against each job already in the queue
    pub fn enable_merge(mut self, f: fn(J, &mut J) -> MergeResult<J>) -> Self {
        self.merge_fn = Some(f);
        self
    }
}

impl<J: Job + Send + RecurrableJob + 'static> Builder<J> {
    /// Set a job as recurring, the job will be enqueued every time `interval` passes since the `last_enqueue` of a matching job
    pub fn set_recurring(mut self, interval: Duration, last_enqueue: Instant, job: J) -> Self {
        self.recurring.push(Box::new(IntervalRecurringJob {
            last_enqueue,
            interval,
            job,
        }));
        self
    }
}

impl<J: Job + Send + 'static> Default for Builder<J> {
    fn default() -> Self {
        Self::new()
    }
}

impl<J: Job + Send + 'static> Builder<J> {
    /// Function determining, for each priority, how many threads can be allocated to jobs of this priority, any remaining threads will be left idle to service higher-priority jobs. `None` means parallelism won't be limited
    pub fn limit_concurrency(
        mut self,
        concurrency_limit: impl Fn(<J as Job>::Priority) -> Option<u8> + Send + Sync + 'static,
    ) -> Self {
        self.concurrency_limit = Box::new(concurrency_limit);
        self
    }

    /// Build the [`JobRunner`], spawning `thread_num` threads as workers
    pub fn build(self, thread_num: usize) -> JobRunner<J> {
        let (sender, sources) =
            SourceManager::<J, Box<dyn RecurringJob<J> + Send>>::new_with_recurring(
                self.recurring,
                self.merge_fn,
            );
        let jobs = Arc::new(Mutex::new(sources));
        let _threads = runner::spawn(thread_num, jobs, self.concurrency_limit);
        JobRunner { sender }
    }
}

/// A job which can be executed by the runner, with features to synchronise jobs that would interfere with each other and reduce the parallelisation of low priority jobs
pub trait Job: Send {
    /// Type used to check which jobs should not be allowed to run concurrently, see [`Job::exclusion()`]. Use [`NoExclusion`] for jobs which can always be run at the same time, see also [`ExclusionOption`].
    type Exclusion: PartialEq + Copy + fmt::Debug + Send;

    /// Used to check which jobs should not be allowed to run concurrently, if `<Job::Exclusion as PartialEq>::eq(job1.exclusion(), job2.exclusion())`, then `job1` and `job2` can't run at the same time.
    fn exclusion(&self) -> Self::Exclusion;

    /// Type of the priority, the higher prioritys are those which are larger based on [`Ord::cmp`].
    type Priority: Ord + Copy + Send;

    /// Get the priority of this thing
    fn priority(&self) -> Self::Priority;

    /// Execute and consume the job
    fn execute(self);
}

impl<T> Job for T
where
    T: FnOnce() + Send,
{
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    type Priority = ();

    fn priority(&self) -> Self::Priority {}

    fn execute(self) {
        (self)()
    }
}

/// Result of an attempted merge, see [`Builder::enable_merge`]
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
