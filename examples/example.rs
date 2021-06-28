///! Example of job scheduler usage.
///!
///! * Jobs block a thread for a pre-defined period of time
///! * Jobs can be added the the queue from `stdin`, one per line `{seconds} [priority] [exclusion]`, ie' `3 5 q`, the later entries can be left out
///! * If it exists, a file `examples/poll` will be read every 10 seconds and jobs created based on the same format (notice that if there are more jobs than threads, the low priority jobs never get scheduled) the jobs should be prioritised with highest first
use std::{
    fs,
    io::{BufRead, BufReader},
    str::FromStr,
    thread,
    time::{Duration, Instant},
};

use foreman::{
    source::{PollError, PollSource, PollableSource},
    ExclusionOption, Job, JobRunner, Prioritised, UnrestrictedParallelism,
};

fn main() {
    let runner = JobRunner::builder()
        .add_poll(PollSource {
            pollable: PollableSource {
                poll: Box::new(poll_file),
                min_interval: Duration::from_secs(20),
                preferred_interval: Duration::from_secs(30),
            },
            last_poll: Instant::now() - Duration::from_secs(120),
        })
        .build(2);

    let stdin = std::io::stdin();
    let mut input = String::new();
    while let Ok(_) = stdin.read_line(&mut input) {
        if let Ok(job) = input.parse() {
            runner.send(job).unwrap()
        }
        input.clear();
    }
}

fn poll_file() -> Result<Box<dyn Iterator<Item = WaitJob>>, PollError> {
    println!("polling");
    let file = fs::File::open("examples/poll").or(Err(PollError::ResetInterval))?;
    let r = BufReader::new(file);
    let jobs = r
        .lines()
        .take_while(Result::is_ok)
        .map(Result::unwrap)
        .flat_map(|line| line.parse());
    Ok(Box::new(jobs))
}

#[derive(Debug)]
struct WaitJob(Duration, u8, Option<char>);

impl FromStr for WaitJob {
    type Err = ();

    fn from_str(line: &str) -> Result<WaitJob, ()> {
        let mut split = line.trim().split_whitespace();
        if let Some(duration) = split.next() {
            match duration.parse() {
                Ok(duration) => {
                    let mut job = WaitJob(Duration::from_secs(duration), 1, None);
                    if let Some(priority) = split.next().and_then(|token| token.parse().ok()) {
                        job.1 = priority;
                    }
                    job.2 = split.next().and_then(|token| token.parse().ok());
                    return Ok(job);
                }
                errs => println!("Errors parsing duration {:?}: {:?}", duration, errs),
            }
        }
        Err(())
    }
}

impl Job for WaitJob {
    type Exclusion = ExclusionOption<char>;

    fn exclusion(&self) -> Self::Exclusion {
        self.2.into()
    }

    fn assigned(&mut self) {}

    fn execute(self) {
        thread::sleep(self.0);
        println!("Completed job {:?}", self);
    }
}

impl Prioritised for WaitJob {
    type Priority = UnrestrictedParallelism<u8>;

    fn priority(&self) -> Self::Priority {
        self.1.into()
    }
}
