// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements how task are pushed and polled. Threads are
//! woken up when new tasks arrived and go to sleep when there are no
//! tasks waiting to be handled.

use crate::pool::SchedConfig;
use crate::queue::{Extras, LocalQueue, Pop, TaskCell, TaskInjector, WithExtras};
use fail::fail_point;
use parking_lot_core::{ParkResult, ParkToken, UnparkToken};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// An usize is used to trace the threads that are working actively.
/// To save additional memory and atomic operation, the number and
/// shutdown hint are merged into one number in the following format
/// ```text
/// 0...00
/// ^    ^
/// |    The least significant bit indicates whether the queue is shutting down.
/// Bits represent the thread count
/// ```
const SHUTDOWN_BIT: usize = 1;
const ENABLED_COUNT_SHIFT: usize = 1;
/// Checks if shutdown bit is set.
pub fn is_shutdown(runtime_settings: usize) -> bool {
    runtime_settings & SHUTDOWN_BIT == SHUTDOWN_BIT
}

pub fn enabled_count(runtime_settings: usize) -> usize {
    runtime_settings >> ENABLED_COUNT_SHIFT
}

/// The core of queues.
///
/// Every thread pool instance should have one and only `QueueCore`. It's
/// saved in an `Arc` and shared between all worker threads and remote handles.
pub(crate) struct QueueCore<T> {
    global_queue: TaskInjector<T>,
    runtime_settings: AtomicUsize,
    active_workers: AtomicUsize,
    backup_workers: AtomicUsize,
    config: SchedConfig,
}

impl<T> QueueCore<T> {
    pub fn new(global_queue: TaskInjector<T>, config: SchedConfig) -> QueueCore<T> {
        QueueCore {
            global_queue,
            runtime_settings: AtomicUsize::new(config.max_thread_count << ENABLED_COUNT_SHIFT),
            active_workers: AtomicUsize::new(config.max_thread_count),
            backup_workers: AtomicUsize::new(0),
            config,
        }
    }

    /// Ensures there are enough workers to handle pending tasks.
    ///
    /// If the method is going to wake up any threads, source is used to trace who triggers
    /// the action.
    pub fn ensure_workers(&self, source: usize) {
        let active_workers = self.active_workers.load(Ordering::SeqCst);
        let runtime_settings = self.runtime_settings.load(Ordering::SeqCst);
        if active_workers >= enabled_count(runtime_settings) || is_shutdown(runtime_settings) {
            return;
        }

        let backup = self.backup_workers.load(Ordering::SeqCst)
            > (self.config.max_thread_count - enabled_count(runtime_settings));
        self.unpark_one(backup, source);
    }

    fn unpark_one(&self, backup: bool, source: usize) {
        unsafe {
            parking_lot_core::unpark_one(self.park_address(backup), |_| UnparkToken(source));
        }
    }

    pub fn park_address(&self, backup: bool) -> usize {
        if backup {
            self as *const QueueCore<T> as usize + 1
        } else {
            self as *const QueueCore<T> as usize
        }
    }

    /// Sets the shutdown bit and notify all threads.
    ///
    /// `source` is used to trace who triggers the action.
    pub fn mark_shutdown(&self, source: usize) {
        self.active_workers.fetch_or(SHUTDOWN_BIT, Ordering::SeqCst);
        let addr = self as *const QueueCore<T> as usize;
        unsafe {
            parking_lot_core::unpark_all(addr, UnparkToken(source));
        }
    }

    /// Checks if the thread pool is shutting down.
    pub fn is_shutdown(&self) -> bool {
        is_shutdown(self.runtime_settings.load(Ordering::SeqCst))
    }

    /// Marks the current thread in sleep state.
    ///
    /// It can be marked as sleep only when the pool is not shutting down.
    pub fn mark_sleep(&self, backup: bool) -> bool {
        let runtime_settings = self.runtime_settings.load(Ordering::SeqCst);
        if is_shutdown(runtime_settings) {
            return false;
        }
        self.active_workers.fetch_sub(1, Ordering::SeqCst);
        if backup {
            self.backup_workers.fetch_add(1, Ordering::SeqCst);
        }
        true
    }

    /// Marks current thread as woken up states.
    pub fn mark_woken(&self, backup: bool) {
        self.active_workers.fetch_add(1, Ordering::SeqCst);
        if backup {
            self.backup_workers.fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn set_active_thread_count(&self, thread_count: usize) {
        let mut runtime_settings = self.runtime_settings.load(Ordering::Relaxed);
        loop {
            if is_shutdown(runtime_settings) {
                return;
            }
            let new_settings = thread_count << ENABLED_COUNT_SHIFT;
            match self.runtime_settings.compare_exchange_weak(
                runtime_settings,
                new_settings,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(n) => runtime_settings = n,
            }
        }
    }
}

impl<T: TaskCell + Send> QueueCore<T> {
    /// Pushes the task to global queue.
    ///
    /// `source` is used to trace who triggers the action.
    fn push(&self, source: usize, task: T) {
        self.global_queue.push(task);
        self.ensure_workers(source);
    }

    fn default_extras(&self) -> Extras {
        self.global_queue.default_extras()
    }
}

/// Submits tasks to associated thread pool.
///
/// Note that thread pool can be shutdown and dropped even not all remotes are
/// dropped.
pub struct Remote<T> {
    core: Arc<QueueCore<T>>,
}

impl<T: TaskCell + Send> Remote<T> {
    pub(crate) fn new(core: Arc<QueueCore<T>>) -> Remote<T> {
        Remote { core }
    }

    /// Submits a task to the thread pool.
    pub fn spawn(&self, task: impl WithExtras<T>) {
        let t = task.with_extras(|| self.core.default_extras());
        self.core.push(0, t);
    }

    pub(crate) fn stop(&self) {
        self.core.mark_shutdown(0);
    }

    /// Sets the count of active threads in the thread pool dynamically.
    pub fn set_active_thread_count(&self, thread_count: usize) {
        self.core.set_active_thread_count(thread_count);
    }
}

impl<T> Clone for Remote<T> {
    fn clone(&self) -> Remote<T> {
        Remote {
            core: self.core.clone(),
        }
    }
}

/// Note that implements of Runner assumes `Remote` is `Sync` and `Send`.
/// So we need to use assert trait to ensure the constraint at compile time
/// to avoid future breaks.
trait AssertSync: Sync {}
impl<T: Send> AssertSync for Remote<T> {}
trait AssertSend: Send {}
impl<T: Send> AssertSend for Remote<T> {}

/// Spawns tasks to the associated thread pool.
///
/// It's different from `Remote` because it submits tasks to the local queue
/// instead of global queue, so new tasks can take advantage of cache
/// coherence.
pub struct Local<T> {
    id: usize,
    local_queue: LocalQueue<T>,
    core: Arc<QueueCore<T>>,
}

impl<T: TaskCell + Send> Local<T> {
    pub(crate) fn new(id: usize, local_queue: LocalQueue<T>, core: Arc<QueueCore<T>>) -> Local<T> {
        Local {
            id,
            local_queue,
            core,
        }
    }

    /// Spawns a task to the local queue.
    pub fn spawn(&mut self, task: impl WithExtras<T>) {
        let t = task.with_extras(|| self.local_queue.default_extras());
        self.local_queue.push(t);
    }

    /// Spawns a task to the remote queue.
    pub fn spawn_remote(&self, task: impl WithExtras<T>) {
        let t = task.with_extras(|| self.local_queue.default_extras());
        self.core.push(self.id, t);
    }

    /// Gets a remote so that tasks can be spawned from other threads.
    pub fn remote(&self) -> Remote<T> {
        Remote {
            core: self.core.clone(),
        }
    }

    pub(crate) fn core(&self) -> &Arc<QueueCore<T>> {
        &self.core
    }

    pub(crate) fn pop(&mut self) -> Option<Pop<T>> {
        self.local_queue.pop()
    }

    /// Pops a task from the queue.
    ///
    /// If there are no tasks at the moment, it will go to sleep until woken
    /// up by other threads.
    pub(crate) fn pop_or_sleep(&mut self) -> Option<Pop<T>> {
        let backup = self.core().backup_workers.load(Ordering::SeqCst)
            < (self.core().config.max_thread_count
                - enabled_count(self.core.runtime_settings.load(Ordering::SeqCst)));
        let address = self.core().park_address(backup);
        let mut task = None;
        let id = self.id;

        let res = unsafe {
            parking_lot_core::park(
                address,
                || {
                    task = self.local_queue.pop();
                    if task.is_some() {
                        return false;
                    }
                    self.core.mark_sleep(backup)
                },
                || {},
                |_, _| {},
                ParkToken(id),
                None,
            )
        };
        match res {
            ParkResult::Unparked(_) | ParkResult::Invalid => {
                self.core.mark_woken(backup);
                task
            }
            ParkResult::TimedOut => unreachable!(),
        }
    }

    /// Returns whether there are preemptive tasks to run.
    ///
    /// If the pool is not busy, other tasks should not preempt the current running task.
    pub(crate) fn need_preempt(&mut self) -> bool {
        fail_point!("need-preempt", |r| { r.unwrap().parse().unwrap() });
        self.local_queue.has_tasks_or_pull()
    }
}

/// Building remotes and locals from the given queue and configuration.
///
/// This is only for tests purpose so that a thread pool doesn't have to be
/// spawned to test a Runner.
pub fn build_spawn<T>(
    queue_type: impl Into<crate::queue::QueueType>,
    config: SchedConfig,
) -> (Remote<T>, Vec<Local<T>>)
where
    T: TaskCell + Send,
{
    let queue_type = queue_type.into();
    let (global, locals) = crate::queue::build(queue_type, config.max_thread_count);
    let core = Arc::new(QueueCore::new(global, config));
    let l = locals
        .into_iter()
        .enumerate()
        .map(|(i, l)| Local::new(i + 1, l, core.clone()))
        .collect();
    let g = Remote::new(core);
    (g, l)
}
