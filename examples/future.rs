//! Example of sending future jobs via queue
//!
//! Schedules some jobs which wait for 1 second, obtaining a future which completes with a result from the job, when a merged job completes, all of the futures for all of the merged jobs complete together
use gaffer::{
    future::{Promise, PromiseFuture},
    Job, JobRunner, MergeResult, NoExclusion,
};
use std::time::Duration;

use futures::{executor::block_on, FutureExt, StreamExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = JobRunner::builder()
        .enable_merge(|this: ProcessString, that: &mut ProcessString| {
            if this.0[..this.0.len() - 1] == that.0[..that.0.len() - 1] {
                that.0 = format!("{}x", &that.0[..that.0.len() - 1]);
                that.1.merge(this.1);
                MergeResult::Success
            } else {
                MergeResult::NotMerged(this)
            }
        })
        .build(1);

    let mut futures: futures::stream::SelectAll<_> = (10..=50)
        .filter_map(|i| {
            ProcessString::new(format!("Job {}", i), &runner)
                .ok()
                .map(|f| f.into_stream())
        })
        .collect();
    println!("Jobs enqueued");

    block_on(async {
        while let Some(result) = futures.next().await {
            let processed_string = result.unwrap();
            println!(">> {}", processed_string);
        }
    });
    Ok(())
}

struct ProcessString(String, Promise<String>);

impl ProcessString {
    fn new(
        name: String,
        runner: &JobRunner<ProcessString>,
    ) -> Result<PromiseFuture<String>, crossbeam_channel::SendError<ProcessString>> {
        let (promise, future) = Promise::new();
        runner.send(ProcessString(name, promise))?;
        Ok(future)
    }
}

impl Job for ProcessString {
    type Exclusion = NoExclusion;

    fn exclusion(&self) -> Self::Exclusion {
        NoExclusion
    }

    type Priority = ();

    fn priority(&self) -> Self::Priority {}

    fn execute(self) {
        println!("Processing job {}", self.0);
        std::thread::sleep(Duration::from_secs(1));
        self.1.fulfill(format!("Processed : [{}]", self.0));
    }
}
