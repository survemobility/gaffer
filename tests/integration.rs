use std::{
    collections::HashSet,
    fmt,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use foreman::*;

#[test]
fn integration_1_thread_blocked() {
    let helper = TestHelper::new(1, Duration::from_millis(10), Duration::from_millis(20));

    helper.wait_micros(400, 1, 'a');
    helper.pause(300); // a gets picked up alone, the rest are waiting once a is ready
    helper.wait_micros(10, 1, 'c');
    helper.wait_micros(10, 1, 'd');
    helper.wait_micros(10, 1, 'e');
    helper.wait_micros(10, 3, 'b');
    helper.assert_recv("abcde");
}

#[test]
fn integration_1_thread_lower_than_poll() {
    let helper = TestHelper::new(1, Duration::from_millis(1), Duration::from_millis(2));

    helper.pause(1000); // poll is ready
    helper.wait_micros(10, 1, 'f'); // lower priority, comes after the x
    helper.assert_recv("xf");
}

#[test]
fn integration_1_thread_higher_than_poll() {
    let helper = TestHelper::new(1, Duration::from_millis(1), Duration::from_millis(2));

    helper.pause(1000); // poll is ready
    helper.wait_micros(10, 3, 'g'); // higher priority, comes before the x
    helper.assert_recv("gx");
}

#[test]
fn integration_1_poll_preferred() {
    let helper = TestHelper::new(1, Duration::from_millis(1), Duration::from_millis(2));

    helper.pause(3000); // poll is preferred, comes before the higher priority h
    helper.wait_micros(10, 3, 'h');
    helper.assert_recv("xh");
}

#[test]
fn integration_2_threads_block() {
    let helper = TestHelper::new(2, Duration::from_millis(10), Duration::from_millis(20));

    helper.wait_micros(300, 1, 'a');
    helper.wait_micros(1000, 1, 'e');
    helper.pause(200); // a & e get picked up first, the rest are waiting once a is done and are done in parallel with e
    helper.wait_micros(10, 1, 'c');
    helper.wait_micros(10, 1, 'd');
    helper.wait_micros(10, 3, 'b');
    helper.assert_recv("abcde");
}

#[test]
fn integration_2_threads_lower_than_poll() {
    let helper = TestHelper::new(2, Duration::from_millis(1), Duration::from_millis(2));

    helper.pause(1000); // poll is ready
    helper.wait_micros(10, 1, 'f'); // lower priority, comes after the x & y
    helper.assert_recv_unordered("xy"); // unfortunately the ordering of x & y is non-deterministic as it depends on how quickly the worker thread wakes up
    helper.assert_recv("f");
}

#[test]
fn integration_2_threads_higher_than_poll() {
    let helper = TestHelper::new(2, Duration::from_millis(1), Duration::from_millis(2));

    helper.pause(1000); // poll is ready
    helper.wait_micros(10, 3, 'g'); // higher priority, comes before the x
    helper.assert_recv_unordered("gx"); // unfortunately the ordering of g & x is non-deterministic as it depends on how quickly the worker thread wakes up
}

#[test]
fn integration_2_threads_poll_preferred() {
    let helper = TestHelper::new(2, Duration::from_millis(2), Duration::from_millis(2));

    helper.pause(3000); // poll is preferred, comes before the higher priority h
    helper.wait_micros(10, 3, 'h');
    helper.assert_recv_unordered("xy"); // unfortunately the ordering of x & y is non-deterministic as it depends on how quickly the worker thread wakes up
    helper.assert_recv("h");
}

struct TestHelper {
    runner: JobRunner<WaitJob>,
    send: mpsc::Sender<char>,
    recv: mpsc::Receiver<char>,
}

impl TestHelper {
    fn new(thread_num: usize, min_interval: Duration, preferred_interval: Duration) -> Self {
        let (send, recv) = mpsc::channel();

        let runner = JobRunner::builder()
            .add_poll(PollSource {
                pollable: PollableSource {
                    poll: {
                        let send = send.clone();
                        Box::new(move || {
                            let send = send.clone();
                            Ok(Box::new(
                                vec![
                                    {
                                        let send = send.clone();
                                        WaitJob {
                                            created: Instant::now(),
                                            duration: Duration::from_micros(40),
                                            priority: 2,
                                            exclusion: None,
                                            callback: Box::new(move || send.send('x').unwrap()),
                                        }
                                    },
                                    {
                                        let send = send.clone();
                                        WaitJob {
                                            created: Instant::now(),
                                            duration: Duration::from_micros(40),
                                            priority: 2,
                                            exclusion: None,
                                            callback: Box::new(move || send.send('y').unwrap()),
                                        }
                                    },
                                    {
                                        let send = send.clone();
                                        WaitJob {
                                            created: Instant::now(),
                                            duration: Duration::from_micros(40),
                                            priority: 2,
                                            exclusion: None,
                                            callback: Box::new(move || send.send('z').unwrap()),
                                        }
                                    },
                                ]
                                .into_iter(),
                            ))
                        })
                    },
                    min_interval,
                    preferred_interval,
                },
                last_poll: Instant::now(),
            })
            .build(thread_num);
        Self { runner, send, recv }
    }

    fn wait_micros(&self, micros: u64, priority: u8, key: char) {
        let send = self.send.clone();
        self.runner
            .send(WaitJob {
                created: Instant::now(),
                duration: Duration::from_micros(micros),
                priority,
                exclusion: None,
                callback: Box::new(move || send.send(key).unwrap()),
            })
            .unwrap()
    }

    fn pause(&self, micros: u64) {
        thread::sleep(Duration::from_micros(micros));
    }

    fn assert_recv(&self, expect: &str) {
        assert_eq!(
            expect,
            self.recv.iter().take(expect.len()).collect::<String>()
        );
    }

    fn assert_recv_unordered(&self, expect: &str) {
        let mut expect: HashSet<_> = expect.chars().collect();
        while !expect.is_empty() {
            let c = self.recv.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(
                expect.remove(&c),
                "Received char {:?} not in expected set {:?}",
                c,
                expect
            );
        }
    }
}

struct WaitJob {
    created: Instant,
    duration: Duration,
    priority: u8,
    exclusion: Option<char>,
    callback: Box<dyn FnOnce() + Send>,
}

impl Job for WaitJob {
    type Exclusion = ExclusionOption<char>;

    fn exclusion(&self) -> Self::Exclusion {
        self.exclusion.into()
    }

    fn execute(self) {
        thread::sleep(self.duration);
        println!("Completed job {:?}", self);
        (self.callback)();
    }
}

impl Prioritised for WaitJob {
    type Priority = UnrestrictedParallelism<u8>;

    fn priority(&self) -> Self::Priority {
        self.priority.into()
    }
}

impl fmt::Debug for WaitJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WaitJob({:?} took {:?}, {:?}, {:?}, ..)",
            self.duration,
            Instant::now() - self.created,
            self.priority,
            self.exclusion
        )
    }
}
