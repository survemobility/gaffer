//! Example of executing jobs in parallel
//!
//! Schedules some jobs which wait for 1 second across 10 threads
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = gaffer::JobRunner::builder().build(10);

    for i in 1..=50 {
        let name = format!("WaitJob {}", i);
        runner.send(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            println!("Completed job {:?}", name);
        })?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(std::time::Duration::from_secs(7));
    Ok(())
}
