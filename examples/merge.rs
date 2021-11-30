//! Example of sending jobs via queue and having them automatically merge
//!
//! Schedules many jobs which wait for 1 second, most will be merged and won't sleep the thread
use gaffer::{Job, JobRunner, MergeResult, NoExclusion};
use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = JobRunner::builder().enable_merge(merge_jobs).build(1);
    let counter = Arc::new(AtomicU8::new(0));

    for i in 10..=50 {
        runner.send(MergeJob(format!("Job {}", i), counter.clone()))?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(Duration::from_secs(7));
    assert_eq!(counter.load(Ordering::SeqCst), 5);
    Ok(())
}

#[derive(Debug)]
struct MergeJob(String, Arc<AtomicU8>);

impl Job for MergeJob {
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    type Priority = ();

    fn priority(&self) -> Self::Priority {}

    fn execute(self) {
        std::thread::sleep(Duration::from_secs(1));
        println!("Completed job {:?}", self);
        self.1.fetch_add(1, Ordering::SeqCst);
    }
}

fn merge_jobs(this: MergeJob, that: &mut MergeJob) -> MergeResult<MergeJob> {
    if this.0[..this.0.len() - 1] == that.0[..that.0.len() - 1] {
        that.0 = format!("{}x", &that.0[..that.0.len() - 1]);
        MergeResult::Success
    } else {
        MergeResult::NotMerged(this)
    }
}
