// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements how task are pushed and polled. Threads are
//! woken up when new tasks arrived and go to sleep when there are no
//! tasks waiting to be handled.

use crate::pool::SchedConfig;
use crate::queue::{Extras, LocalQueue, Pop, TaskCell, TaskInjector, WithExtras};
use fail::fail_point;
use parking_lot_core::{ParkResult, ParkToken, UnparkToken};
use prometheus::{Histogram, HistogramOpts};
use std::mem::{self, MaybeUninit};
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicIsize, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::Arc;

/// A u64 is used to trace the states of the workers.
/// The highest 16 bits represent the running workers count.
/// The 17-32 bits represent the active workers count.
/// The 33-48 bits represent the backup workers count.
/// The lowest 16 bits are used to save other flags, currently only the lowest
/// bit is used to represent whether the pool has been shut down.
trait WorkersInfo {
    fn new(workers_count: usize) -> Self;

    fn is_shutdown(self) -> bool;

    fn running_count(self) -> usize;

    fn active_count(self) -> usize;

    fn backup_count(self) -> usize;

    // Change an active worker from not running to running.
    fn active_to_running(self) -> Self;

    // Change an running worker to a not running but active worker.
    fn running_to_active(self) -> Self;

    // Change an running worker to a backup worker.
    fn running_to_backup(self) -> Self;

    // Change a backup worker to a running worker.
    fn backup_to_running(self) -> Self;
}

const SHUTDOWN_BIT: u64 = 1;
const RUNNING_COUNT_SHIFT: u32 = 48;
const ACTIVE_COUNT_SHIFT: u32 = 32;
const BACKUP_COUNT_SHIFT: u32 = 16;
const COUNT_MASK: u64 = 0xFFFF;

impl WorkersInfo for u64 {
    fn new(workers_count: usize) -> u64 {
        let workers_count = workers_count as u64;
        assert!(workers_count <= COUNT_MASK, "too many workers");
        (workers_count << ACTIVE_COUNT_SHIFT) | (workers_count << RUNNING_COUNT_SHIFT)
    }

    fn is_shutdown(self) -> bool {
        self & SHUTDOWN_BIT == SHUTDOWN_BIT
    }

    fn running_count(self) -> usize {
        ((self >> RUNNING_COUNT_SHIFT) & COUNT_MASK) as usize
    }

    fn active_count(self) -> usize {
        ((self >> ACTIVE_COUNT_SHIFT) & COUNT_MASK) as usize
    }

    fn backup_count(self) -> usize {
        ((self >> BACKUP_COUNT_SHIFT) & COUNT_MASK) as usize
    }

    fn active_to_running(self) -> Self {
        debug_assert!(self.is_shutdown() || self.running_count() < self.active_count());
        self + (1 << RUNNING_COUNT_SHIFT)
    }

    fn running_to_active(self) -> Self {
        debug_assert!(self.is_shutdown() || self.running_count() > 0);
        self - (1 << RUNNING_COUNT_SHIFT)
    }

    fn running_to_backup(self) -> Self {
        self - (1 << RUNNING_COUNT_SHIFT) - (1 << ACTIVE_COUNT_SHIFT) + (1 << BACKUP_COUNT_SHIFT)
    }

    fn backup_to_running(self) -> Self {
        self + (1 << RUNNING_COUNT_SHIFT) + (1 << ACTIVE_COUNT_SHIFT) - (1 << BACKUP_COUNT_SHIFT)
    }
}

/// The core of queues.
///
/// Every thread pool instance should have one and only `QueueCore`. It's
/// saved in an `Arc` and shared between all worker threads and remote handles.
pub(crate) struct QueueCore<T> {
    global_queue: TaskInjector<T>,
    workers_info: AtomicU64,
    idling: AtomicU64,
    stats: UtilizationStats,
    active_notified: AtomicBool,
    backup_notified: AtomicBool,
    config: SchedConfig,
}

impl<T> QueueCore<T> {
    pub fn new(global_queue: TaskInjector<T>, config: SchedConfig) -> QueueCore<T> {
        QueueCore {
            global_queue,
            workers_info: AtomicU64::new(WorkersInfo::new(config.max_thread_count)),
            idling: AtomicU64::new(0),
            active_notified: AtomicBool::new(false),
            backup_notified: AtomicBool::new(false),
            stats: UtilizationStats::new(config.max_thread_count),
            config,
        }
    }

    /// Ensures there are enough workers to handle pending tasks.
    ///
    /// If the method is going to wake up any threads, source is used to trace who triggers
    /// the action.
    pub fn ensure_workers(&self, source: usize) {
        let workers_info = self.workers_info.load(Ordering::SeqCst);
        if workers_info.is_shutdown() {
            return;
        }

        if workers_info.active_count() < self.config.min_thread_count {
            self.unpark_one(true, source);
        } else if workers_info.running_count() < self.config.min_thread_count {
            self.unpark_one(false, source);
        } else if source != 0 {
            self.stats.add_record(
                workers_info
                    .running_count()
                    .saturating_sub(self.idling.load(Ordering::SeqCst) as usize),
            );
            if self.stats.average() > (workers_info.active_count() - 1) as f64 {
                self.unpark_one(true, source);
            }
        } else if workers_info.running_count() < workers_info.active_count() {
            self.unpark_one(false, source);
        }
    }

    pub fn unpark_one(&self, backup: bool, source: usize) {
        let notified = if backup {
            &self.backup_notified
        } else {
            &self.active_notified
        };
        if !notified.compare_and_swap(false, true, Ordering::SeqCst) {
            unsafe {
                parking_lot_core::unpark_one(self.park_address(backup), |_| UnparkToken(source));
            }
        }
    }

    pub fn park_to_backup(&self) -> bool {
        let workers_info = self.workers_info.load(Ordering::SeqCst);
        self.stats.average() < (workers_info.active_count() - 2) as f64
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
        self.workers_info.fetch_or(1, Ordering::SeqCst);
        unsafe {
            parking_lot_core::unpark_all(self.park_address(false), UnparkToken(source));
            parking_lot_core::unpark_all(self.park_address(true), UnparkToken(source));
        }
    }

    /// Checks if the thread pool is shutting down.
    pub fn is_shutdown(&self) -> bool {
        self.workers_info.load(Ordering::SeqCst).is_shutdown()
    }

    /// Marks the current thread in sleep state.
    ///
    /// It can be marked as sleep only when the pool is not shutting down.
    pub fn mark_sleep(&self, backup: bool, active_count_histogram: &Histogram) -> bool {
        let notified = if backup {
            &self.backup_notified
        } else {
            &self.active_notified
        };
        if notified.compare_and_swap(true, false, Ordering::SeqCst) {
            return false;
        }
        let mut workers_info = self.workers_info.load(Ordering::SeqCst);
        loop {
            if workers_info.is_shutdown() {
                return false;
            }

            let new_info = if backup {
                workers_info.running_to_backup()
            } else {
                workers_info.running_to_active()
            };
            match self.workers_info.compare_exchange_weak(
                workers_info,
                new_info,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if backup {
                        active_count_histogram.observe((workers_info.active_count() - 1) as f64);
                    }
                    return true;
                }
                Err(actual) => workers_info = actual,
            }
        }
    }

    /// Marks current thread as woken up states.
    pub fn mark_woken(&self, backup: bool) {
        let notified = if backup {
            &self.backup_notified
        } else {
            &self.active_notified
        };
        notified.store(false, Ordering::SeqCst);
        let mut workers_info = self.workers_info.load(Ordering::SeqCst);
        loop {
            let new_info = if backup {
                workers_info.backup_to_running()
            } else {
                workers_info.active_to_running()
            };
            match self.workers_info.compare_exchange_weak(
                workers_info,
                new_info,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return;
                }
                Err(actual) => workers_info = actual,
            }
        }
    }

    pub fn mark_idling(&self) {
        self.idling.fetch_add(1, Ordering::SeqCst);
    }

    pub fn unmark_idling(&self) {
        self.idling.fetch_sub(1, Ordering::SeqCst);
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

struct UtilizationStats {
    records: [AtomicUsize; 256],
    sum: AtomicIsize,
    index: AtomicU8,
}

impl UtilizationStats {
    fn new(max_thread_count: usize) -> UtilizationStats {
        let mut records: [MaybeUninit<AtomicUsize>; 256] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for r in &mut records[..] {
            *r = MaybeUninit::new(AtomicUsize::new(max_thread_count));
        }
        UtilizationStats {
            records: unsafe { mem::transmute(records) },
            sum: AtomicIsize::new(max_thread_count as isize * 256),
            index: AtomicU8::new(0),
        }
    }

    fn add_record(&self, running_count: usize) {
        let index = self.index.fetch_add(1, Ordering::SeqCst) as usize;
        let old = self.records[index].swap(running_count, Ordering::SeqCst);
        let diff = running_count as isize - old as isize;
        self.sum.fetch_add(diff, Ordering::SeqCst);
    }

    fn average(&self) -> f64 {
        self.sum.load(Ordering::SeqCst) as f64 / 256.0
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
    pub(crate) id: usize,
    local_queue: LocalQueue<T>,
    core: Arc<QueueCore<T>>,
    active_count_histogram: Histogram,
}

impl<T: TaskCell + Send> Local<T> {
    pub(crate) fn new(
        id: usize,
        local_queue: LocalQueue<T>,
        core: Arc<QueueCore<T>>,
        active_count_histogram: Histogram,
    ) -> Local<T> {
        Local {
            id,
            local_queue,
            core,
            active_count_histogram,
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
        let backup = self.core.park_to_backup();
        let mut task = None;
        let id = self.id;
        let mut marked_sleep = false;

        let res = unsafe {
            parking_lot_core::park(
                self.core.park_address(backup),
                || {
                    if !self.core.mark_sleep(backup, &self.active_count_histogram) {
                        return false;
                    }
                    marked_sleep = true;
                    task = self.local_queue.pop();
                    task.is_none()
                },
                || {},
                |_, _| {},
                ParkToken(id),
                None,
            )
        };
        match res {
            ParkResult::Unparked(_) | ParkResult::Invalid => {
                if marked_sleep {
                    self.core.mark_woken(backup);
                }
                self.core.ensure_workers(id);
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
    let backup_counter = Histogram::with_opts(HistogramOpts::new("_", "_")).unwrap();
    let l = locals
        .into_iter()
        .enumerate()
        .map(|(i, l)| Local::new(i + 1, l, core.clone(), backup_counter.clone()))
        .collect();
    let g = Remote::new(core);
    (g, l)
}
