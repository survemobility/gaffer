# gaffer

Prioritised, parallel job scheduler with concurrent exclusion, job merging, recurring jobs and load limiting for lower priorities.

A job scheduler executes tasks on it's own thread or thread pool. This job scheduler is particularly designed to consider heavier weight or more expensive jobs, which likely have side effects. In this case it can be valuable to prioritise the jobs and merge alike jobs in the queue.

__Features__

* Recurring jobs: jobs which will be re-enqueued at some interval
* Job queue: use an `crossbeam_channel::Sender<YourJob>` to send jobs
* Future Jobs: (Optionally) create `Future`s to get results from the jobs
* Job prioritisation: provide a priority for jobs and all the jobs will be executed in that order
* Job merging: merge identical / similar jobs in the queue to reduce workload
* Parallel execution: run jobs on multiple threads and lock jobs which should be run exclusively, they remain in the queue and don't occupy other resources
* Concurrent exclusion: key-based locking to avoid jobs running concurrently which shouldn't
* Priority throttling: in order to have idle threads ready to pick up higher-priority jobs, throttle lower priority jobs by restricting them to a lower number of threads

__Limitations__

* some of the tests are very dependent on timing and will fail if run slowly

## Usage

With `cargo-edit`:

```sh
cargo add gaffer
```

or in `Cargo.toml`

```toml
[dependencies]
gaffer = "0.2"
```

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.

