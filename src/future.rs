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

pub struct PromiseFutureMulti<T> {
    shared: Arc<PromiseShared<T>>,
    waker_index: Option<usize>,
}

impl<T: Clone> Clone for PromiseFutureMulti<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            waker_index: None,
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
    wakers: Vec<Waker>, // the usual case is to have 0 or 1 wakers, more will only happen when merging. Ideally the first waker wouldn't require an allocation
}

impl<T> Default for PromiseInner<T> {
    fn default() -> Self {
        Self {
            result: None,
            wakers: Vec::with_capacity(1),
        }
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        self.shared.promise_dropped.store(true, Ordering::Release);
        for waker in self.shared.inner.lock().unwrap().wakers.drain(..) {
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

    fn new_multi() -> (Promise<T>, PromiseFutureMulti<T>) {
        let shared = Default::default();
        (
            Self {
                shared: Arc::clone(&shared),
            },
            PromiseFutureMulti {
                shared,
                waker_index: None,
            },
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
            data.wakers.clear();
            data.wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T: Clone> Future for PromiseFutureMulti<T> {
    type Output = Result<T, PromiseDropped>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let is_last = Arc::strong_count(&self.shared) == 1;
        let mut data = self.shared.inner.lock().unwrap();
        if let Some(result) = &data.result {
            Poll::Ready(Ok(if is_last {
                data.result.take().unwrap() // avoid unnecessary clone
            } else {
                result.clone()
            }))
        } else if self.shared.promise_dropped.load(Ordering::Acquire) {
            Poll::Ready(Err(PromiseDropped))
        } else {
            if let Some(waker_idx) = self.waker_index {
                data.wakers[waker_idx] = cx.waker().clone();
            } else {
                let waker_idx = data.wakers.len();
                data.wakers.push(cx.waker().clone());
                drop(data);
                self.waker_index.replace(waker_idx);
            }
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
    fn test_multi_with_multi_result() {
        let (promise, fut1) = Promise::new_multi();
        let fut2 = fut1.clone();
        let job = MyJob(promise, "hello".to_string());
        thread::spawn(move || job.execute());
        assert_eq!(Ok("hello".to_string()), block_on(fut1));
        assert_eq!(Ok("hello".to_string()), block_on(fut2));
    }

    #[test]
    fn test_multi_with_multi_result_already() {
        let (promise, fut1) = Promise::new_multi();
        let fut2 = fut1.clone();
        let job = MyJob(promise, "hello".to_string());
        job.execute();
        assert_eq!(Ok("hello".to_string()), block_on(fut1));
        assert_eq!(Ok("hello".to_string()), block_on(fut2));
    }

    #[test]
    fn test_multi_with_multi_drop() {
        let (promise, fut1) = Promise::new_multi();
        let fut2 = fut1.clone();
        let job = MyJob(promise, "hello".to_string());
        thread::spawn(move || drop(job));
        assert_eq!(Err(PromiseDropped), block_on(fut1));
        assert_eq!(Err(PromiseDropped), block_on(fut2));
    }

    #[test]
    fn test_multi_with_multi_drop_already() {
        let (promise, fut1) = Promise::new_multi();
        let fut2 = fut1.clone();
        let job = MyJob(promise, "hello".to_string());
        drop(job);
        assert_eq!(Err(PromiseDropped), block_on(fut1));
        assert_eq!(Err(PromiseDropped), block_on(fut2));
    }

    #[test]
    fn test_multi_with_result() {
        let (promise, fut) = Promise::new_multi();
        let job = MyJob(promise, "hello".to_string());
        thread::spawn(move || job.execute());
        assert_eq!(Ok("hello".to_string()), block_on(fut));
    }

    #[test]
    fn test_multi_with_result_already() {
        let (promise, fut) = Promise::new_multi();
        let job = MyJob(promise, "hello".to_string());
        job.execute();
        assert_eq!(Ok("hello".to_string()), block_on(fut));
    }

    #[test]
    fn test_multi_with_drop() {
        let (promise, fut) = Promise::new_multi();
        let job = MyJob(promise, "hello".to_string());
        thread::spawn(move || drop(job));
        assert_eq!(Err(PromiseDropped), block_on(fut));
    }

    #[test]
    fn test_multi_with_drop_already() {
        let (promise, fut) = Promise::new_multi();
        let job = MyJob(promise, "hello".to_string());
        drop(job);
        assert_eq!(Err(PromiseDropped), block_on(fut));
    }
}
