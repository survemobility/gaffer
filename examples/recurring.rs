//! Example of recurring jobs
//!
//! Configured with a job which reccurs every 2 seconds
use gaffer::{Builder, Job, MergeResult, NoExclusion, Prioritised};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _runner = Builder::new()
        .set_recurring(
            Duration::from_secs(2),
            std::time::Instant::now(),
            MyJob("recurring"),
        )
        .build(1);

    std::thread::sleep(Duration::from_secs(7));
    Ok(())
}

#[derive(Debug, Clone)]
struct MyJob(&'static str);

impl Job for MyJob {
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    fn execute(self) {
        println!("Completed job {:?}", self);
    }
}

/// This Job isn't actually prioritised but this trait needs to be implemented for now
impl Prioritised for MyJob {
    type Priority = ();

    fn priority(&self) -> Self::Priority {}

    const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> = None;

    /// matches needs to be implemented for recurring jobs, it must return `true` for a `.clone()` of it's self
    fn matches(&self, job: &Self) -> bool {
        self.0 == job.0
    }
}
