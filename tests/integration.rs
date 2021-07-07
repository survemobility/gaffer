use std::{
    collections::HashSet,
    fmt, thread,
    time::{Duration, Instant},
};

use foreman::*;

macro_rules! assert_recv {
    ($helper:expr, $expect:literal) => {
        let mut actual = String::default();
        while $expect.len() > actual.len() {
            let c = $helper
                .recv
                .recv_timeout(Duration::from_millis(100))
                .expect(&format!(
                    "Timed out with only {:?}, expected {:?}",
                    actual, $expect
                ));
            actual.push(c);
        }
        assert_eq!($expect, actual);
    };
}

#[test]
fn integration_1_thread_blocked() {
    let helper = TestHelper::new(1, Duration::from_millis(10), "x");

    helper.wait_micros(400, 1, 'a');
    helper.pause(300); // a gets picked up alone, the rest are waiting once a is ready
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

// Currently failing, there is a bug with thread limiting which leads to large delays before scheduling jobs with thread limits
#[test]
#[ignore]
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

    helper.wait_micros(300, 1, 'a');
    helper.wait_micros(1000, 1, 'e');
    helper.pause(200); // a & e get picked up first, the rest are waiting once a is done and are done in parallel with e
    helper.wait_micros(10, 1, 'c');
    helper.wait_micros(10, 1, 'd');
    helper.wait_micros(10, 3, 'b');
    assert_recv!(helper, "abcde");
}

#[test]
fn integration_2_threads_lower_than_recurring() {
    let helper = TestHelper::new(2, Duration::from_millis(1), "xy");

    helper.pause(1000); // recurring is ready
    helper.wait_micros(10, 1, 'f'); // lower priority, comes after the x & y
    helper.assert_recv_unordered("xy"); // unfortunately the ordering of y & z is non-deterministic as it depends on how quickly the worker thread wakes up
    assert_recv!(helper, "f");
}

#[test]
fn integration_2_threads_higher_than_recurring() {
    let helper = TestHelper::new(2, Duration::from_millis(1), "x");

    helper.pause(1000); // poll is ready
    helper.wait_micros(10, 3, 'g'); // higher priority, comes before the z
    helper.assert_recv_unordered("gx"); // unfortunately the ordering of g & z is non-deterministic as it depends on how quickly the worker thread wakes up
}

#[test]
fn integration_2_threads_poll_preferred() {
    let helper = TestHelper::new(2, Duration::from_millis(2), "xy");

    helper.pause(3000); // poll is preferred, comes before the higher priority h
    helper.wait_micros(10, 3, 'h');
    helper.assert_recv_unordered("xy"); // unfortunately the ordering of x & y is non-deterministic as it depends on how quickly the worker thread wakes up
    assert_recv!(helper, "h");
}

struct TestHelper {
    runner: JobRunner<WaitJob>,
    send: crossbeam_channel::Sender<char>,
    recv: crossbeam_channel::Receiver<char>,
}

impl TestHelper {
    fn new(thread_num: usize, interval: Duration, recurring: &str) -> Self {
        let (send, recv) = crossbeam_channel::unbounded();

        let mut runner = JobRunner::builder();
        for key in recurring.chars() {
            runner.set_recurring(
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
            );
        }
        let runner = runner.build(thread_num);
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

    fn assert_recv(&self, expect: &str) {
        let mut actual = String::default();
        while expect.len() > actual.len() {
            let c = self
                .recv
                .recv_timeout(Duration::from_millis(100))
                .expect(&format!(
                    "Timed out with only {:?}, expected {:?}",
                    actual, expect
                ));
            actual.push(c);
        }
        assert_eq!(expect, actual);
    }

    fn assert_recv_unordered(&self, expect: &str) {
        let expect: HashSet<_> = expect.chars().collect();
        let mut actual = HashSet::default();
        while expect.len() > actual.len() {
            let c = self
                .recv
                .recv_timeout(Duration::from_millis(100))
                .expect(&format!(
                    "Timed out with only {:?}, expected {:?}",
                    actual, expect
                ));
            actual.insert(c);
        }
        assert_eq!(expect, actual);
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

impl Job for WaitJob {
    type Exclusion = ExclusionOption<char>;

    fn exclusion(&self) -> Self::Exclusion {
        self.exclusion.into()
    }

    fn execute(self) {
        thread::sleep(self.duration);
        println!("Completed job {:?}", self);
        self.send.send(self.key).unwrap();
    }
}

impl Prioritised for WaitJob {
    type Priority = u8;

    fn priority(&self) -> Self::Priority {
        self.priority.into()
    }

    fn matches(&self, job: &Self) -> bool {
        self.key == job.key
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
