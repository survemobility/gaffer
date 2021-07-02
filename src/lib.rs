///! # foreman
///!
///! Prioritised, parallel job scheduler with concurrent exclusion, job merging, job source polling and load limiting for lower priorities.
///!
///! A job scheduler executes tasks on it's own thread or thread pool. This job scheduler is particularly designed to consider heavier weight or more expensive jobs, which likely have side effects.
///!
///! __Features__
///!
///! * Job polling: provide a function to poll & a polling interval range and foreman will poll within that range for jobs
///! * Job queue: use an `std::sync::mpsc::Sender<YourJob>` to send jobs
///! * Future Jobs: (Optionally) create `Future`s to get results from the jobs
///! * Job prioritisation: provide a priority for jobs and all the jobs will be executed in that order
///! * Job merging: merge identical / similar jobs in the queue to reduce workload
///! * Parallel execution: run jobs on multiple threads and lock jobs which should be run exclusively, they remain in the queue and don't occupy other resources
///! * Priority throttling: in order to have idle threads ready to pick up higher-priority jobs, throttle lower priority jobs by restricting them to a lower number of threads
///! * No dependencies: only because no crates were found providing ready-built components that worked how they needed to for this crate
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
///! ### Job polling
///!
///! Provide a function to poll & a polling interval range and foreman will poll within that range for jobs
///!
///! ```rust
///! fn main() {
///!     JobRunner::builder()
///!         .add_poll(PollSource {
///!             pollable: PollableSource {
///!                 poll: Box::new(|| Ok(Box::new(vec![].into_iter())) ), // provide how to obtain the jobs
///!                 min_interval: Duration::from_secs(20), // the poll function won't be called until at least this has elapsed past the `last_poll`
///!                 preferred_interval: Duration::from_secs(30), // if the supervisor is idle, it will wait this long past `last_poll` before making a poll
///!             },
///!             last_poll: Instant::now() - Duration::from_secs(120), // updated each poll
///!         })
///!         .build(2);
///! }
///! ```
///!
///! ## Usage
///!
///! ```toml
///! [dependencies]
///! foreman = { git = "ssh://git@github.com/survemobility/foreman.git", branch = "pr-1" }
///! ```
use std::{
    fmt,
    sync::{mpsc, Arc, Mutex},
};

use runner::ConcurrencyLimitFn;
use source::SourceManager;
pub use source::{PollError, PollSource, PollableSource};

pub mod future;
mod runner;
pub mod source;

pub struct JobRunner<J: Job + 'static> {
    sender: mpsc::Sender<J>,
}

impl<J: Job + 'static> JobRunner<J> {
    pub fn builder() -> Builder<J> {
        Builder {
            poll_sources: vec![],
            concurrency_limit: Box::new(|_: <J as Prioritised>::Priority| None as Option<u8>),
        }
    }

    pub fn send(&self, job: J) -> Result<(), mpsc::SendError<J>> {
        self.sender.send(job)
    }
}

pub struct Builder<J: Job + 'static> {
    poll_sources: Vec<PollSource<J>>,
    concurrency_limit: Box<ConcurrencyLimitFn<J>>,
}

impl<J: Job + 'static> Builder<J> {
    /// Poller that will be polled according to it's own schedule to find candidate jobs. Any
    pub fn add_poll(mut self, pollable: PollSource<J>) -> Self {
        self.poll_sources.push(pollable);
        self
    }

    /// Function determining, for each priority, how many threads can be allocated to jobs of this priority, any remaining threads will be left idle to service higher-priority jobs. `None` means parallelism won't be limited
    pub fn limit_concurrency(
        mut self,
        concurrency_limit: impl Fn(<J as Prioritised>::Priority) -> Option<u8> + Send + Sync + 'static,
    ) -> Self {
        self.concurrency_limit = Box::new(concurrency_limit);
        self
    }

    pub fn build(self, thread_num: usize) -> JobRunner<J> {
        let (sender, sources) = SourceManager::new(self.poll_sources);
        let jobs = Arc::new(Mutex::new(sources));
        let _threads = runner::spawn(thread_num, jobs, self.concurrency_limit);
        JobRunner { sender }
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

/// Allows some jobs to be run at the same time, and others to be exclusive
#[derive(Debug, Copy, Clone)]
pub enum ExclusionOption<T> {
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
