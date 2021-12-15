use std::{
    collections::HashSet,
    fmt, thread,
    time::{Duration, Instant},
};

use crossbeam_channel::Sender;
use gaffer::*;

const TIMEOUT: Duration = Duration::from_millis(100);

macro_rules! assert_recv {
    ($helper:expr, $expect:literal) => {
        let mut actual = String::default();
        while $expect.len() > actual.len() {
            let c = $helper.recv.recv_timeout(TIMEOUT).expect(&format!(
                "Timed out after {:?} with only {:?}, expected {:?}",
                TIMEOUT, actual, $expect
            ));
            actual.push(c);
        }
        assert_eq!($expect, actual);
    };
}

macro_rules! assert_recv_unordered {
    ($helper:expr, $expect:literal) => {
        let expect: HashSet<_> = $expect.chars().collect();
        let mut actual = String::default();
        while expect.len() > actual.len() {
            let c = $helper.recv.recv_timeout(TIMEOUT).expect(&format!(
                "Timed out after {:?} with only {:?}, expected {:?}",
                TIMEOUT, actual, $expect
            ));
            actual.push(c);
        }
        assert_eq!(
            expect,
            actual.chars().collect::<HashSet<_>>(),
            "{:?} didn't match {:?}",
            $expect,
            actual
        );
    };
}

#[test]
fn integration_1_thread_blocked() {
    let helper = TestHelper::new(1, Duration::from_millis(10), "x");

    helper.wait_micros(400, 1, 'a');
    helper.pause(200); // a gets picked up alone, the rest are waiting once a is ready
    helper.wait_micros(10, 1, 'c');
    helper.wait_micros(10, 1, 'd');
    helper.wait_micros(10, 1, 'e');
    helper.wait_micros(10, 3, 'b');
    assert_recv!(helper, "abcde");
}

#[test]
fn integration_1_thread_recurrance() {
    let helper = TestHelper::new(1, Duration::from_millis(1), "xyz");

    assert_recv!(helper, "xyz");
    assert_recv!(helper, "xyz");
    helper.wait_micros(10, 3, 'a');
    helper.wait_micros(10, 3, 'b');
    assert_recv!(helper, "ab");
    assert_recv!(helper, "xyz");
}

#[test]
fn integration_2_thread_limited() {
    let helper =
        TestHelper::new_runner(JobRunner::builder().limit_concurrency(|_| Some(1)).build(2));

    helper.wait_micros(10, 3, 'a');
    helper.wait_micros(10, 3, 'b');
    helper.wait_micros(10, 3, 'c');
    helper.wait_micros(10, 3, 'd');
    assert_recv!(helper, "abcd");
}

#[test]
fn integration_2_threads_block() {
    let helper = TestHelper::new(2, Duration::from_millis(10), "x");

    helper.wait_micros(2000, 1, 'a');
    helper.wait_micros(5000, 1, 'e');
    helper.pause(1000); // a & e get picked up first, the rest are waiting once a is done and are done in parallel with e
    helper.wait_micros(10, 1, 'c');
    helper.wait_micros(10, 1, 'd');
    helper.wait_micros(10, 3, 'b');
    assert_recv!(helper, "abcde");
}

#[test]
fn integration_2_threads_poll_preferred() {
    let helper = TestHelper::new(2, Duration::from_millis(2), "xy");

    helper.pause(3000); // poll is preferred, comes before the higher priority h
    helper.wait_micros(10, 3, 'h');
    assert_recv_unordered!(helper, "xy"); // unfortunately the ordering of x & y is non-deterministic as it depends on how quickly the worker thread wakes up
    assert_recv!(helper, "h");
}

// a panicking job should not kill the thread
#[test]
fn panic_in_job() {
    let (send, recv) = crossbeam_channel::unbounded();

    struct PanicJob(Option<Sender<()>>);
    impl Job for PanicJob {
        type Priority = ();

        fn priority(&self) -> Self::Priority {}

        type Exclusion = NoExclusion;

        fn exclusion(&self) -> Self::Exclusion {
            NoExclusion
        }

        fn execute(self) {
            if let Some(send) = self.0 {
                send.send(()).unwrap();
            } else {
                panic!();
            }
        }
    }
    let runner = JobRunner::builder().build(1);
    runner.send(PanicJob(None)).unwrap();
    runner.send(PanicJob(Some(send))).unwrap();
    assert!(recv.recv_timeout(Duration::from_millis(500)).is_ok());
    thread::sleep(Duration::from_millis(1));
}

struct TestHelper {
    runner: JobRunner<WaitJob>,
    send: crossbeam_channel::Sender<char>,
    recv: crossbeam_channel::Receiver<char>,
}

impl TestHelper {
    fn new(thread_num: usize, interval: Duration, recurring: &str) -> Self {
        let (send, recv) = crossbeam_channel::unbounded();

        let mut builder = gaffer::JobRunner::builder();
        for key in recurring.chars() {
            builder = builder.set_recurring(
                interval,
                Instant::now(),
                WaitJob {
                    created: Instant::now(),
                    duration: Duration::from_micros(40),
                    priority: 2,
                    exclusion: None,
                    key,
                    send: send.clone(),
                },
            )
        }
        let runner = builder.build(thread_num);
        Self { runner, send, recv }
    }

    fn new_runner(runner: JobRunner<WaitJob>) -> Self {
        let (send, recv) = crossbeam_channel::unbounded();
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
                key,
                send,
            })
            .unwrap()
    }

    fn pause(&self, micros: u64) {
        thread::sleep(Duration::from_micros(micros));
    }
}

#[derive(Clone)]
struct WaitJob {
    created: Instant,
    duration: Duration,
    priority: u8,
    exclusion: Option<char>,
    key: char,
    send: crossbeam_channel::Sender<char>,
}

impl RecurrableJob for WaitJob {
    fn matches(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Job for WaitJob {
    type Exclusion = ExclusionOption<char>;

    fn exclusion(&self) -> Self::Exclusion {
        self.exclusion.into()
    }

    type Priority = u8;

    fn priority(&self) -> Self::Priority {
        self.priority.into()
    }

    fn execute(self) {
        thread::sleep(self.duration);
        println!("Completed job {:?}", self);
        self.send.send(self.key).unwrap();
    }
}

impl fmt::Debug for WaitJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WaitJob({:?} took {:?}, {:?}, {:?}, {:?}, ..)",
            self.duration,
            Instant::now() - self.created,
            self.priority,
            self.exclusion,
            self.key,
        )
    }
}
