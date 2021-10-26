//! Example of sending jobs via queue
//!
//! Schedules some jobs which wait for 1 second
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = gaffer::JobRunner::builder().build(1);

    for i in 1..=5 {
        let name = format!("Job {}", i);
        runner.send(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            println!("Completed job {:?}", name);
        })?;
    }

    println!("Jobs enqueued");
    std::thread::sleep(std::time::Duration::from_secs(7));
    Ok(())
}
