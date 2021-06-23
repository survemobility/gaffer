use std::fmt;

mod runner;
mod source;

/// A job which can be executed by the runner, with features to synchronise jobs that would interfere with each other and reduce the parallelisation of low priority jobs
pub trait Job: Prioritised {
    type Exclusion: PartialEq + Copy + fmt::Debug;

    /// exclude jobs which can't be run concurrently. if .`exclusion()` matches for 2 jobs, the runner won't run them at the same time
    fn exclusion(&self) -> Self::Exclusion;

    /// called by the runner supervisor when assigned to a worker, immediately before execution
    fn assigned(&mut self);

    fn execute(self);
}

pub trait Prioritised {
    type Priority: Priority;

    fn priority(&self) -> Self::Priority;
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
struct UnrestrictedParallelism<T: Ord + Copy>(T);
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
