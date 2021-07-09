///! Example of job scheduler usage.
///!
///! * Jobs block a thread for a pre-defined period of time
///! * Jobs can be added the the queue from `stdin`, one per line `{seconds} [priority] [exclusion]`, ie' `3 5 q`, the later entries can be left out
///! * If it exists, a file `examples/poll` will be read every 10 seconds and jobs created based on the same format (notice that if there are more jobs than threads, the low priority jobs never get scheduled) the jobs should be prioritised with highest first
use std::{
    error::Error,
    fs,
    io::{BufRead, BufReader},
    str::FromStr,
    thread,
    time::{Duration, Instant},
};

use chief::{ExclusionOption, Job, JobRunner, MergeResult, Prioritised};

fn main() -> Result<(), Box<dyn Error>> {
    let mut runner = JobRunner::builder();
    let file = fs::File::open("examples/poll")?;
    let r = BufReader::new(file);
    for line in r.lines().take_while(Result::is_ok).flat_map(Result::ok) {
        let (interval, job) = line
            .split_once(' ')
            .ok_or("jobs in poll file must consists of at least {interval} {duration}")?;
        let interval = Duration::from_secs(interval.parse()?);
        let job: WaitJob = job.parse()?;
        println!("Recurring every {:?} : {:?}", interval, job);
        runner.set_recurring(interval, Instant::now(), job);
    }

    let runner = runner.build(2);

    let stdin = std::io::stdin();
    let mut input = String::new();
    while let Ok(_) = stdin.read_line(&mut input) {
        if input == "\n" {
            return Ok(());
        }
        if let Ok(job) = input.parse() {
            runner.send(job)?;
        } else {
            println!("Couldnt parse {:?}", input);
        }
        input.clear();
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct WaitJob(Duration, u8, Option<char>);

impl Job for WaitJob {
    type Exclusion = ExclusionOption<char>;

    fn exclusion(&self) -> Self::Exclusion {
        self.2.into()
    }

    fn execute(self) {
        thread::sleep(self.0);
        println!("Completed job {:?}", self);
    }
}

impl Prioritised for WaitJob {
    type Priority = u8;

    fn priority(&self) -> Self::Priority {
        self.1.into()
    }

    const ATTEMPT_MERGE_INTO: Option<fn(Self, &mut Self) -> MergeResult<Self>> =
        Some(|me: Self, other: &mut Self| -> MergeResult<Self> {
            if me.2.is_some() && me.2 == other.2 {
                other.0 += me.0;
                other.1 = other.1.max(me.1);
                MergeResult::Success
            } else {
                MergeResult::NotMerged(me)
            }
        });

    fn matches(&self, job: &Self) -> bool {
        self.2.is_some() && self.2 == job.2
    }
}

impl FromStr for WaitJob {
    type Err = &'static str;

    fn from_str(line: &str) -> Result<WaitJob, Self::Err> {
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
        Err("Failed to parse WaitJob")
    }
}
