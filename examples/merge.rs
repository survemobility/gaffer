//! Example of sending jobs via queue and having them automatically merge
//!
//! Schedules many jobs which wait for 1 second, most will be merged and won't sleep the thread
use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = Builder::new().build(1);

    for i in 10..=50 {
        runner.send(MergeJob(format!("Job {}", i)))?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(Duration::from_secs(7));
    Ok(())
}

#[derive(Debug, Clone)]
struct MergeJob(String);

impl Job for MergeJob {
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    fn execute(self) {
        std::thread::sleep(Duration::from_secs(1));
        println!("Completed job {:?}", self);
    }
}

/// This Job isn't actually prioritised but this trait implements the job merge
impl Prioritised for MergeJob {
    type Priority = ();

    fn priority(&self) -> Self::Priority {}

    const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> =
        Some(|this, that| {
            if this.matches(that) {
                that.0 = format!("{}x", &that.0[..that.0.len() - 1]);
                MergeResult::Success
            } else {
                MergeResult::NotMerged(this)
            }
        });

    fn matches(&self, that: &Self) -> bool {
        self.0[..self.0.len() - 1] == that.0[..that.0.len() - 1]
    }
}
