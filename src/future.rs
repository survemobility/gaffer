use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Poll, Waker};

pub struct Promise<T> {
    shared: Arc<PromiseShared<T>>,
}
pub struct PromiseFuture<T> {
    shared: Arc<PromiseShared<T>>,
}

impl<T: Clone> Clone for PromiseFuture<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

// struct Promise<T>(Arc<Mutex<(Option<T>, Option<Waker>)>>);
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

#[derive(Debug)]
struct PromiseInner<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

impl<T> Default for PromiseInner<T> {
    fn default() -> Self {
        Self {
            result: None,
            waker: None,
        }
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        self.shared.promise_dropped.store(true, Ordering::Release);
        if let Some(waker) = self.shared.inner.lock().unwrap().waker.take() {
            waker.wake();
        }
    }
}

unsafe impl<T: Send> Send for PromiseInner<T> {}
unsafe impl<T: Send> Sync for PromiseInner<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct PromiseDropped;

impl<T> Promise<T> {
    fn new() -> (Promise<T>, PromiseFuture<T>) {
        let shared = Default::default();
        (
            Self {
                shared: Arc::clone(&shared),
            },
            PromiseFuture { shared },
        )
    }

    fn set_value(self, result: T) {
        let mut data = self.shared.inner.lock().unwrap();
        // println!("Settign value on {:?}", (&data.result, &data.waker));
        data.result.replace(result);
    }
}

impl<T> Future for PromiseFuture<T> {
    type Output = Result<T, PromiseDropped>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut data = self.shared.inner.lock().unwrap();
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

    use crate::{Job, Prioritised};

    use super::*;

    struct MyJob(Promise<String>, String);

    impl Job for MyJob {
        type Exclusion = ();

        fn exclusion(&self) -> Self::Exclusion {}

        fn assigned(&mut self) {}

        fn execute(self) {
            self.0.set_value(self.1)
        }
    }

    impl Prioritised for MyJob {
        type Priority = ();

        fn priority(&self) -> Self::Priority {}
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
    fn test_multi_with_result() {
        let (promise, fut1) = Promise::new();
        let fut2 = fut1.clone();
        let job = MyJob(promise, "hello".to_string());
        thread::spawn(move || job.execute());
        assert_eq!(Ok("hello".to_string()), block_on(fut1));
        assert_eq!(Ok("hello".to_string()), block_on(fut2));
    }
}
