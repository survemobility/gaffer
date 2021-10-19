//! Example of prioritised jobs in the queue being executed in parallel with throttling
//!
//! Schedules some prioritised jobs which wait for 1 second, the higher priority jobs run across 4 threads and the lower on only 1
use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = Builder::new()
        .limit_concurrency(|priority| (priority == 1).then(|| 1))
        .build(4);

    for (i, priority) in (1..=10).zip([1, 2].iter().cycle()) {
        runner.send(PrioritisedJob(format!("Job {}", i), *priority))?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(Duration::from_secs(7));
    Ok(())
}

#[derive(Debug, Clone)]
struct PrioritisedJob(String, u8);

impl Job for PrioritisedJob {
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    fn execute(self) {
        std::thread::sleep(Duration::from_secs(1));
        println!("Completed job {:?}", self);
    }
}

impl Prioritised for PrioritisedJob {
    type Priority = u8;

    /// This Job is prioritied
    fn priority(&self) -> Self::Priority {
        self.1
    }

    const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;

    fn matches(&self, _job: &Self) -> bool {
        false
    }
}
