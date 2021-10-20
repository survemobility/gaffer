//! Example of excluded jobs in the queue
//!
//! Jobs with an exclusion wont run concurrently with another job with the same key, jobs with no exclusion can run with unlimited concurrency
use gaffer::{ExclusionOption, Job, JobRunner};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = JobRunner::builder().build(2);

    for (i, exclusion) in (1..=10).zip(
        [
            ExclusionOption::Some(1),
            ExclusionOption::Some(2),
            ExclusionOption::None,
        ]
        .iter()
        .cycle(),
    ) {
        runner.send(ExcludedJob(format!("Job {}", i), *exclusion))?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(Duration::from_secs(7));
    Ok(())
}

#[derive(Debug)]
struct ExcludedJob(String, ExclusionOption<u8>);

impl Job for ExcludedJob {
    type Exclusion = ExclusionOption<u8>;

    fn exclusion(&self) -> Self::Exclusion {
        self.1
    }

    type Priority = ();

    fn priority(&self) -> Self::Priority {}

    fn execute(self) {
        std::thread::sleep(Duration::from_secs(1));
        println!("Completed job {:?}", self);
    }
}
