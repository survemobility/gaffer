//! Example of prioritised jobs in the queue
//!
//! Schedules some prioritised jobs which wait for 1 second
use gaffer::{Job, JobRunner, NoExclusion};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = JobRunner::builder().build(1);

    for (i, priority) in (1..=5).zip([1, 2].iter().cycle()) {
        runner.send(PrioritisedJob(format!("Job {}", i), *priority))?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(Duration::from_secs(7));
    Ok(())
}

#[derive(Debug)]
struct PrioritisedJob(String, u8);

impl Job for PrioritisedJob {
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    type Priority = u8;

    /// This Job is prioritied
    fn priority(&self) -> Self::Priority {
        self.1
    }

    fn execute(self) {
        std::thread::sleep(Duration::from_secs(1));
        println!("Completed job {:?}", self);
    }
}
