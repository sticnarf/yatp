// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::spawn::QueueCore;
use crate::pool::{Local, Runner};
use crate::queue::{Pop, TaskCell};
use parking_lot_core::{SpinWait, UnparkToken};
use std::sync::atomic::spin_loop_hint;
use std::sync::atomic::Ordering::*;
use std::time::{Duration, Instant};

pub(crate) struct WorkerThread<T, R> {
    local: Local<T>,
    runner: R,
    pop_count: u64,
}

impl<T, R> WorkerThread<T, R> {
    pub fn new(local: Local<T>, runner: R) -> WorkerThread<T, R> {
        WorkerThread {
            local,
            runner,
            pop_count: 0,
        }
    }
}

impl<T, R> WorkerThread<T, R>
where
    T: TaskCell + Send,
    R: Runner<TaskCell = T>,
{
    #[inline]
    fn pop(&mut self) -> Option<Pop<T>> {
        // Wait some time before going to sleep, which is more expensive.
        // let mut spin = SpinWait::new();
        self.pop_count += 1;
        let mut counter = 0;
        // let mut idling = false;
        // let mut start = None;
        loop {
            // if !idling {
            //     if self
            //         .local
            //         .core()
            //         .idling
            //         .compare_and_swap(false, true, SeqCst)
            //     {
            //         break;
            //     } else {
            //         idling = true;
            //     }
            // }
            if let Some(t) = self.local.pop() {
                // self.local.core().idling.store(false, SeqCst);
                if self.local.core().notified.load(SeqCst) {
                    let addr = self.local.core().as_ref() as *const QueueCore<T> as usize;
                    unsafe {
                        parking_lot_core::unpark_one(addr, |_| UnparkToken(self.local.id));
                    }
                }
                return Some(t);
            }
            if counter < 3 {
                std::thread::yield_now();
            } else if counter < 5 {
                std::thread::sleep(Duration::from_micros(10));
            } else {
                // self.local.core().idling.store(false, SeqCst);
                break;
            }
            counter += 1;
            // if !spin.spin() {
            //     self.local.core().idling.store(false, SeqCst);
            //     self.idle_count += 1;
            //     self.idle_time += start.unwrap().elapsed();
            //     break;
            // }
        }
        self.runner.pause(&mut self.local);
        let t = self.local.pop_or_sleep();
        self.runner.resume(&mut self.local);
        t
    }

    pub fn run(mut self) {
        self.runner.start(&mut self.local);
        while !self.local.core().is_shutdown() {
            let task = match self.pop() {
                Some(t) => t,
                None => continue,
            };
            self.runner.handle(&mut self.local, task.task_cell);
        }
        self.runner.end(&mut self.local);
        // println!(
        //     "total time: {:?}, idle count: {}, pop count: {}, skip count: {}, park count: {}",
        //     self.idle_time, self.idle_count, self.pop_count, self.skip_count, self.local.park_count
        // );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::spawn::*;
    use crate::queue::QueueType;
    use crate::task::callback;
    use std::sync::*;
    use std::time::*;

    #[derive(Default, PartialEq, Debug)]
    struct Metrics {
        start: usize,
        handle: usize,
        pause: usize,
        resume: usize,
        end: usize,
    }

    struct Runner {
        runner: callback::Runner,
        metrics: Arc<Mutex<Metrics>>,
        tx: mpsc::Sender<()>,
    }

    impl crate::pool::Runner for Runner {
        type TaskCell = callback::TaskCell;

        fn start(&mut self, local: &mut Local<Self::TaskCell>) {
            self.metrics.lock().unwrap().start += 1;
            self.runner.start(local);
        }

        fn handle(&mut self, local: &mut Local<Self::TaskCell>, t: Self::TaskCell) -> bool {
            self.metrics.lock().unwrap().handle += 1;
            self.runner.handle(local, t)
        }

        /// Called when the runner is put to sleep.
        fn pause(&mut self, local: &mut Local<Self::TaskCell>) -> bool {
            self.metrics.lock().unwrap().pause += 1;
            let b = self.runner.pause(local);
            let _ = self.tx.send(());
            b
        }

        /// Called when the runner is woken up.
        fn resume(&mut self, local: &mut Local<Self::TaskCell>) {
            self.metrics.lock().unwrap().resume += 1;
            self.runner.resume(local)
        }

        /// Called when the runner is about to be destroyed.
        ///
        /// It's guaranteed that no other method will be called after this method.
        fn end(&mut self, local: &mut Local<Self::TaskCell>) {
            self.metrics.lock().unwrap().end += 1;
            self.runner.end(local)
        }
    }

    #[test]
    fn test_hooks() {
        let (tx, rx) = mpsc::channel();
        let r = Runner {
            runner: callback::Runner::default(),
            metrics: Default::default(),
            tx: tx.clone(),
        };
        let metrics = r.metrics.clone();
        let mut expected_metrics = Metrics::default();
        let (injector, mut locals) = build_spawn(QueueType::SingleLevel, Default::default());
        let th = WorkerThread::new(locals.remove(0), r);
        let handle = std::thread::spawn(move || {
            th.run();
        });
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        expected_metrics.start = 1;
        expected_metrics.pause = 1;
        assert_eq!(expected_metrics, *metrics.lock().unwrap());

        injector.spawn(move |_: &mut callback::Handle<'_>| {});
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        expected_metrics.pause = 2;
        expected_metrics.handle = 1;
        expected_metrics.resume = 1;
        assert_eq!(expected_metrics, *metrics.lock().unwrap());

        injector.stop();
        handle.join().unwrap();
        expected_metrics.resume = 2;
        expected_metrics.end = 1;
        assert_eq!(expected_metrics, *metrics.lock().unwrap());
    }
}
