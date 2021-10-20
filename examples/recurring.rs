//! Example of recurring jobs
//!
//! Configured with a job which reccurs every 2 seconds
use gaffer::{Job, JobRunner, NoExclusion, RecurrableJob};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _runner = JobRunner::builder()
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

    type Priority = ();

    fn priority(&self) -> Self::Priority {}
}

impl RecurrableJob for MyJob {
    fn matches(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
