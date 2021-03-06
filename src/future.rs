//! Promises which can be used by a job to complete a future from another thread. Promises can also be merged, a merged promise completes all the merged futures at the same time.
//!
//! # Example
//!
//! ```
//! use gaffer::future::Promise;
//!
//! let (mut promise1, future1) = Promise::new();
//! let (promise2, future2) = Promise::new();
//! promise1.merge(promise2);
//! std::thread::spawn(move || promise1.fulfill("hello"));
//!
//! assert_eq!(futures::executor::block_on(future2), Ok("hello"));
//! assert_eq!(futures::executor::block_on(future1), Ok("hello"));
//! ```

use parking_lot::{Mutex, MutexGuard};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Poll, Waker},
};

/// The sending side of a promise which can be used to complete a future. If 2 promises are of the same type, they can be merged and then all the futures will be resolved with clones of the result.
pub struct Promise<T> {
    shared: Arc<PromiseShared<T>>,
}

/// The receiving side of a promise which will be fulfilled by another thread. Unlike a regular [`std::future::Future`] if this is dropped the computation will continue. If the other end is dropped, this will complete with `Err(PromiseDropped)`
pub struct PromiseFuture<T> {
    shared: Arc<PromiseShared<T>>,
}

struct PromiseShared<T> {
    inner: Mutex<PromiseInner<T>>,
    promise_dropped: AtomicBool,
}

impl<T> Default for PromiseShared<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            promise_dropped: Default::default(),
        }
    }
}

impl<T> PromiseShared<T> {
    fn inner(&self) -> MutexGuard<'_, PromiseInner<T>> {
        self.inner.lock()
    }
}

struct PromiseInner<T> {
    result: Option<T>,
    waker: Option<Waker>,
    merged: Option<Promise<T>>,
}

impl<T> Default for PromiseInner<T> {
    fn default() -> Self {
        Self {
            result: None,
            waker: None,
            merged: None,
        }
    }
}

unsafe impl<T: Send> Send for PromiseInner<T> {}
unsafe impl<T: Send> Sync for PromiseInner<T> {}

/// The promise was dropped and so a result will never be provided
#[derive(Debug, PartialEq, Eq)]
pub struct PromiseDropped;

impl std::error::Error for PromiseDropped {}

impl std::fmt::Display for PromiseDropped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("The promise was dropped without being fulfilled")
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        self.shared.promise_dropped.store(true, Ordering::Release);
        let mut data = self.shared.inner();
        drop(data.merged.take());
        if let Some(waker) = data.waker.take() {
            waker.wake();
        }
    }
}

impl<T> Promise<T> {
    /// Create the sending and receiving parts of the promise.
    pub fn new() -> (Promise<T>, PromiseFuture<T>) {
        let shared = Default::default();
        (
            Self {
                shared: Arc::clone(&shared),
            },
            PromiseFuture { shared },
        )
    }

    /// Fulfill the promise, the future will be woken and can retrieve the result, if used on a merged [Promise] the merged futures will receive [PromiseDropped]. So prefer [Promise::fulfill] on mergable results.
    pub fn fulfill_unmergable(self, result: T) {
        let mut data = self.shared.inner();
        data.result.replace(result);
    }
}

impl<T: Clone> Promise<T> {
    /// Fulfill the promise, the future will be woken and can retrieve the result. Any merged Promises will be fulfilled at the same time
    pub fn fulfill(self, result: T) {
        let mut data = self.shared.inner();
        if let Some(merged) = data.merged.take() {
            merged.fulfill(result.clone());
        }
        data.result.replace(result);
    }

    /// Merge another promise of the same type into this promise, the future paired with the other promise will be linked to this and will be completed when this one is fulfilled or dropped.
    pub fn merge(&mut self, other: Self) {
        let mut data = self.shared.inner();
        if let Some(merged) = &mut data.merged {
            merged.merge(other);
        } else {
            data.merged.replace(other);
        }
    }
}

impl<T> Future for PromiseFuture<T> {
    type Output = Result<T, PromiseDropped>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut data = self.shared.inner();
        if let Some(result) = data.result.take() {
            Poll::Ready(Ok(result))
        } else if self.shared.promise_dropped.load(Ordering::Acquire) {
            Poll::Ready(Err(PromiseDropped))
        } else {
            data.waker.replace(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use futures::executor::block_on;

    use crate::{Job, MergeResult};

    use super::*;

    struct MyJob(Promise<String>, String);

    impl Job for MyJob {
        type Exclusion = ();

        fn exclusion(&self) -> Self::Exclusion {}

        type Priority = ();

        fn priority(&self) -> Self::Priority {}

        fn execute(self) {
            self.0.fulfill(self.1)
        }
    }

    fn merge(this: MyJob, target: &mut MyJob) -> crate::MergeResult<MyJob> {
        target.1.push_str(&this.1);
        target.0.merge(this.0);
        MergeResult::Success
    }

    #[test]
    fn test_with_result() {
        let (promise, fut) = Promise::new();
        let job = MyJob(promise, "hello".to_string());
        thread::spawn(move || job.execute());
        assert_eq!(Ok("hello".to_string()), block_on(fut));
    }

    #[test]
    fn test_with_result_already() {
        let (promise, fut) = Promise::new();
        let job = MyJob(promise, "hello".to_string());
        job.execute();
        assert_eq!(Ok("hello".to_string()), block_on(fut));
    }

    #[test]
    fn test_with_drop() {
        let (promise, fut) = Promise::new();
        let job = MyJob(promise, "hello".to_string());
        thread::spawn(move || drop(job));
        assert_eq!(Err(PromiseDropped), block_on(fut));
    }

    #[test]
    fn test_with_drop_already() {
        let (promise, fut) = Promise::new();
        let job = MyJob(promise, "hello".to_string());
        drop(job);
        assert_eq!(Err(PromiseDropped), block_on(fut));
    }

    #[test]
    fn test_merged_with_result() {
        let (promise1, fut1) = Promise::new();
        let (promise2, fut2) = Promise::new();
        let mut job = MyJob(promise1, "hello".to_string());
        let job2 = MyJob(promise2, "world".to_string());
        merge(job2, &mut job);
        thread::spawn(move || job.execute());
        assert_eq!(Ok("helloworld".to_string()), block_on(fut1));
        assert_eq!(Ok("helloworld".to_string()), block_on(fut2));
    }

    #[test]
    fn test_merged_with_result_reverse() {
        let (promise1, fut1) = Promise::new();
        let (promise2, fut2) = Promise::new();
        let mut job = MyJob(promise1, "hello".to_string());
        let job2 = MyJob(promise2, "world".to_string());
        merge(job2, &mut job);
        thread::spawn(move || job.execute());
        assert_eq!(Ok("helloworld".to_string()), block_on(fut2));
        assert_eq!(Ok("helloworld".to_string()), block_on(fut1));
    }

    #[test]
    fn test_merged_with_drop() {
        let (promise1, fut1) = Promise::new();
        let (promise2, fut2) = Promise::new();
        let mut job = MyJob(promise1, "hello".to_string());
        let job2 = MyJob(promise2, "world".to_string());
        merge(job2, &mut job);
        thread::spawn(move || drop(job));
        assert!(block_on(fut1).is_err());
        assert!(block_on(fut2).is_err());
    }

    #[test]
    fn test_merged_with_drop_reverse() {
        let (promise1, fut1) = Promise::new();
        let (promise2, fut2) = Promise::new();
        let mut job = MyJob(promise1, "hello".to_string());
        let job2 = MyJob(promise2, "world".to_string());
        merge(job2, &mut job);
        thread::spawn(move || drop(job));
        assert!(block_on(fut2).is_err());
        assert!(block_on(fut1).is_err());
    }
}
