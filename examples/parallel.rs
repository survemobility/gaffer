//! Example of executing jobs in parallel
//!
//! Schedules some jobs which wait for 1 second across 10 threads
use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = Builder::new().build(10);

    for i in 1..=50 {
        runner.send(WaitJob(format!("Job {}", i)))?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(Duration::from_secs(7));
    Ok(())
}

#[derive(Debug, Clone)]
struct WaitJob(String);

impl Job for WaitJob {
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    fn execute(self) {
        std::thread::sleep(Duration::from_secs(1));
        println!("Completed job {:?}", self);
    }
}

/// This Job isn't actually prioritised but this trait needs to be implemented for now
impl Prioritised for WaitJob {
    type Priority = ();

    fn priority(&self) -> Self::Priority {}

    const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;

    fn matches(&self, _job: &Self) -> bool {
        false
    }
}
