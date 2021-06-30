use std::{
    fmt,
    sync::{mpsc, Arc, Mutex},
    thread::JoinHandle,
};

use source::SourceManager;
pub use source::{PollSource, PollableSource};

pub mod future;
mod runner;
pub mod source;

pub struct JobRunner<J: Job + 'static> {
    threads: Vec<JoinHandle<()>>,
    sender: mpsc::Sender<J>,
}

impl<J: Job + 'static> JobRunner<J> {
    pub fn builder() -> Builder<J> {
        Builder {
            poll_sources: vec![],
        }
    }

    pub fn send(&self, job: J) -> Result<(), mpsc::SendError<J>> {
        self.sender.send(job)
    }
}

pub struct Builder<J: Job + 'static> {
    poll_sources: Vec<PollSource<J>>,
}

impl<J: Job + 'static> Builder<J> {
    pub fn add_poll(mut self, pollable: PollSource<J>) -> Self {
        self.poll_sources.push(pollable);
        self
    }

    pub fn build(self, thread_num: usize) -> JobRunner<J> {
        let (sender, sources) = SourceManager::new(self.poll_sources);
        let jobs = Arc::new(Mutex::new(sources));
        let threads = runner::spawn(thread_num, jobs);
        JobRunner { threads, sender }
    }
}

/// A job which can be executed by the runner, with features to synchronise jobs that would interfere with each other and reduce the parallelisation of low priority jobs
pub trait Job: Prioritised + Send {
    type Exclusion: PartialEq + Copy + fmt::Debug + Send;

    /// exclude jobs which can't be run concurrently. if .`exclusion()` matches for 2 jobs, the runner won't run them at the same time
    fn exclusion(&self) -> Self::Exclusion;

    /// called by the runner supervisor when assigned to a worker, immediately before execution
    fn assigned(&mut self);

    fn execute(self);
}

pub trait Prioritised: Sized {
    type Priority: Priority + Send;

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

pub trait Priority: Ord + Copy {
    /// how many threads can be allocated to jobs of this priority, any remaining threads will be left idle to service higher-priority jobs. `None` means parallelism won't be limited
    fn parrallelism(&self) -> Option<u8>;
}

/// No prioritisation
impl Priority for () {
    fn parrallelism(&self) -> Option<u8> {
        None
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct UnrestrictedParallelism<T: Ord + Copy>(T);
impl<T: Ord + Copy> Priority for UnrestrictedParallelism<T> {
    fn parrallelism(&self) -> Option<u8> {
        None
    }
}
impl<T: Ord + Copy> From<T> for UnrestrictedParallelism<T> {
    fn from(from: T) -> Self {
        UnrestrictedParallelism(from)
    }
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
