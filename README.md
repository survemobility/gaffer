# foreman

Prioritised, parallel job scheduler with concurrent exclusion, job merging, job source polling and load limiting for lower priorities.

A job scheduler executes tasks on it's own thread or thread pool. This job scheduler is particularly designed to consider heavier weight or more expensive jobs, which likely have side effects.

__Features__

* Job polling: provide a function to poll & a polling interval range and foreman will poll within that range for jobs
* Job queue: use an `std::sync::mpsc::Sender<YourJob>` to send jobs
* Future Jobs: (Optionally) create `Future`s to get results from the jobs
* Job prioritisation: provide a priority for jobs and all the jobs will be executed in that order
* Job merging: merge identical / similar jobs in the queue to reduce workload
* Parallel execution: run jobs on multiple threads and lock jobs which should be run exclusively, they remain in the queue and don't occupy other resources
* Priority throttling: in order to have idle threads ready to pick up higher-priority jobs, throttle lower priority jobs by restricting them to a lower number of threads
* No dependencies: only because no crates were found providing ready-built components that worked how they needed to for this crate

## Usage

```toml
[dependencies]
foreman = { git = "ssh://git@github.com/survemobility/foreman.git", branch = "pr-1" }
```
