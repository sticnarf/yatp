// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Yatp is a thread pool that tries to be adaptive, responsive and generic.

mod core;
mod runner;

pub mod queue;
pub mod task;

use crate::core::{
    SchedConfig, SpawnManager, ThreadPoolCore, RUNNING_BASE_SHIFT, SLOTS_BASE, SLOTS_BASE_SHIFT,
};

use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

pub use self::queue::Queue;
pub use self::runner::{LocalSpawn, RemoteSpawn, Runner};

pub struct ThreadPool<Q: Queue> {
    remote: Q::Remote,
    core: Arc<ThreadPoolCore>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<Q: Queue> ThreadPool<Q> {
    pub fn spawn_opt(&self, t: impl Into<Q::Task>, opt: &<Q::Remote as RemoteSpawn>::SpawnOption) {
        self.remote.spawn_opt(t, opt)
    }

    pub fn shutdown(&self) {
        self.core.shutdown();
        let mut threads = mem::replace(&mut *self.threads.lock().unwrap(), Vec::new());
        for _ in 0..threads.len() {
            self.core.unpark_shutdown();
        }
        for j in threads.drain(..) {
            j.join().unwrap();
        }
    }
}

impl<Q: Queue> Drop for ThreadPool<Q> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Clone)]
pub struct Builder {
    name_prefix: String,
    stack_size: Option<usize>,
    sched_config: SchedConfig,
}

impl Builder {
    pub fn new(name_prefix: impl Into<String>) -> Builder {
        Builder {
            name_prefix: name_prefix.into(),
            stack_size: None,
            sched_config: SchedConfig {
                max_thread_count: num_cpus::get(),
                min_thread_count: 1,
                max_inplace_spin: 4,
                max_idle_time: Duration::from_millis(1),
                // In general, when a thread is waken up to handle tasks,
                // it can handle at least one task, then the waken up makes sence.
                // After a thread handles a request, it will sleep again when there
                // are three continous misses. So the miss rate of a reasonalbe woken up
                // should not be higher than 3 / 4.
                tolerate_miss_rate: 3.0 / 4.0,
                max_wait_time: Duration::from_millis(1),
                spawn_backoff: Duration::from_millis(1),
                alloc_slot_backoff: Duration::from_millis(2),
            },
        }
    }

    pub fn max_thread_count(&mut self, count: usize) -> &mut Builder {
        if count > 0 {
            self.sched_config.max_thread_count = count;
        }
        self
    }

    pub fn min_thread_count(&mut self, count: usize) -> &mut Builder {
        if count > 0 {
            self.sched_config.min_thread_count = count;
        }
        self
    }

    pub fn max_inplace_spin(&mut self, count: usize) -> &mut Builder {
        self.sched_config.max_inplace_spin = count;
        self
    }

    pub fn max_idle_time(&mut self, time: Duration) -> &mut Builder {
        self.sched_config.max_idle_time = time;
        self
    }

    pub fn max_wait_time(&mut self, time: Duration) -> &mut Builder {
        self.sched_config.max_wait_time = time;
        self
    }

    pub fn spawn_backoff(&mut self, time: Duration) -> &mut Builder {
        self.sched_config.spawn_backoff = time;
        self
    }

    pub fn alloc_slot_backoff(&mut self, time: Duration) -> &mut Builder {
        self.sched_config.alloc_slot_backoff = time;
        self
    }

    pub fn stack_size(&mut self, size: usize) -> &mut Builder {
        if size > 0 {
            self.stack_size = Some(size);
        }
        self
    }

    // pub fn freeze<G: GlobalQueue>(
    //     &self,
    //     global_ctor: impl FnOnce() -> G,
    // ) -> (Remote<G>, LazyConfig<G>) {
    //     let global = global_ctor();
    //     assert!(self.sched_config.max_thread_count < SLOTS_BASE >> RUNNING_BASE_SHIFT);
    //     assert!(self.sched_config.min_thread_count <= self.sched_config.max_thread_count);
    //     let mut workers = Vec::with_capacity(self.sched_config.max_thread_count);
    //     let mut stealers = Vec::with_capacity(self.sched_config.max_thread_count);
    //     for _ in 0..self.sched_config.max_thread_count {
    //         let w = crossbeam_deque::Worker::new_lifo();
    //         stealers.push(w.stealer());
    //         workers.push(Some(w));
    //     }
    //     let running_info = (self.sched_config.max_thread_count << SLOTS_BASE_SHIFT)
    //         | (self.sched_config.max_thread_count << RUNNING_BASE_SHIFT);
    //     let manager = Arc::new(SpawnManager::new(&self.sched_config));
    //     let queues = Queues {
    //         core: Arc::new(QueueCore {
    //             global,
    //             stealers,
    //             manager,
    //             locals: Mutex::new(workers),
    //             running_info: AtomicUsize::new(running_info),
    //         }),
    //     };
    //     (
    //         Remote {
    //             queue: queues.clone(),
    //         },
    //         LazyConfig {
    //             cfg: self.clone(),
    //             queues,
    //         },
    //     )
    // }

    pub fn build<Q, R>(
        &self,
        mut queue_ctor: impl FnMut() -> Q,
        mut runner_ctor: impl FnMut() -> R,
    ) -> ThreadPool<Q>
    where
        Q: Queue,
        R: Runner<Task = Q::Task, Spawn = Q>,
    {
        assert!(self.sched_config.max_thread_count < SLOTS_BASE >> RUNNING_BASE_SHIFT);
        assert!(self.sched_config.min_thread_count <= self.sched_config.max_thread_count);
        let mut workers = Vec::with_capacity(self.sched_config.max_thread_count);
        let mut stealers = Vec::with_capacity(self.sched_config.max_thread_count);
        for _ in 0..self.sched_config.max_thread_count {
            let w = crossbeam_deque::Worker::new_lifo();
            stealers.push(w.stealer());
            workers.push(Some(w));
        }
        let running_info = (self.sched_config.max_thread_count << SLOTS_BASE_SHIFT)
            | (self.sched_config.max_thread_count << RUNNING_BASE_SHIFT);
        let manager = Arc::new(SpawnManager::new(&self.sched_config));
        let core = Arc::new(ThreadPoolCore::new(
            AtomicUsize::new(running_info),
            manager,
            self.sched_config.max_thread_count,
        ));
        let queues = Queues {
            core: Arc::new(QueueCore {
                global,
                stealers,
                manager,
                locals: Mutex::new(workers),
                running_info: AtomicUsize::new(running_info),
            }),
        };
        unimplemented!()
        // self.freeze(global_ctor).1.spawn(factory)
    }
}
