# chief

Prioritised, parallel job scheduler with concurrent exclusion, job merging, recurring jobs and load limiting for lower priorities.

A job scheduler executes tasks on it's own thread or thread pool. This job scheduler is particularly designed to consider heavier weight or more expensive jobs, which likely have side effects. In this case it can be valuable to prioritise the jobs and merge alike jobs in the queue.

__Features__

* Recurring jobs: jobs which will be re-enqueued at some interval <sup>2</sup>
* Job queue: use an `crossbeam_channel::Sender<YourJob>` to send jobs
* Future Jobs: (Optionally) create `Future`s to get results from the jobs <sup>2</sup>
* Job prioritisation: provide a priority for jobs and all the jobs will be executed in that order
* Job merging: merge identical / similar jobs in the queue to reduce workload <sup>2</sup>
* Parallel execution: run jobs on multiple threads and lock jobs which should be run exclusively, they remain in the queue and don't occupy other resources <sup>1</sup>
* Priority throttling: in order to have idle threads ready to pick up higher-priority jobs, throttle lower priority jobs by restricting them to a lower number of threads <sup>1</sup>
* No dependencies: only because no crates were found providing ready-built components that worked how they needed to for this crate

__Limitations__

* <sup>1</sup> A bug currently limits how quickly a job will get taken up which was delayed by concurrent exclusion or priority throttling. This should be fixed by allowing workers to steal work
* <sup>2</sup> There are a few ergonomics issues to do with the job merging and recurring jobs apis. For example, all jobs need to implement `Clone` (so they can be reproduced for recurring) and any results provided by a future need to implement `Clone` (so that they can be merged).

## Usage

```toml
[dependencies]
chief = { git = "ssh://git@github.com/survemobility/chief.git", branch = "pr-1" }
```
