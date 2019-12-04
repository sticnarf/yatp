// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::queue::{LocalQueue, Pop, TaskInjector};
use crate::runner::{LocalSpawn, RemoteSpawn, Runner, RunnerBuilder};

use parking_lot_core::{FilterOp, UnparkToken};
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const SHUTDOWN_BIT: usize = 0x01;
pub const RUNNING_BASE_SHIFT: usize = 1;
pub const RUNNING_BASE: usize = 1 << RUNNING_BASE_SHIFT;
pub const SLOTS_BASE_SHIFT: usize = 16;
pub const SLOTS_BASE: usize = 1 << SLOTS_BASE_SHIFT;

#[inline]
const fn slot_count(info: usize) -> usize {
    info >> SLOTS_BASE_SHIFT
}

#[inline]
const fn running_count(info: usize) -> usize {
    (info & (SLOTS_BASE - 1)) >> RUNNING_BASE_SHIFT
}

#[inline]
const fn is_slot_full(info: usize) -> bool {
    running_count(info) == slot_count(info)
}

#[inline]
const fn is_slot_empty(info: usize) -> bool {
    running_count(info) == 0
}

#[inline]
const fn is_shutdown(info: usize) -> bool {
    info & SHUTDOWN_BIT == SHUTDOWN_BIT
}

#[derive(Copy, Clone, Default, Debug)]
struct DequeueMetrics {
    hit: usize,
    miss: usize,
}

#[derive(Debug)]
struct ParkMetrics {
    desc_slots: usize,
    incr_slots: usize,
    park_cnt: Vec<usize>,
    unpark_times: usize,
}

impl ParkMetrics {
    fn new(cap: usize) -> ParkMetrics {
        ParkMetrics {
            desc_slots: 0,
            incr_slots: 0,
            park_cnt: vec![0; cap],
            unpark_times: 0,
        }
    }
}

struct BackOff {
    next: AtomicU64,
    backoff: Duration,
}

impl BackOff {
    fn new(backoff: Duration) -> BackOff {
        BackOff {
            next: AtomicU64::new(0),
            backoff,
        }
    }

    fn next_backoff(&self) -> Duration {
        Duration::from_micros(self.next.load(Ordering::Acquire))
    }

    fn should_backoff(&self, curr_dur: Duration, expected_elapsed: &mut Duration) -> bool {
        if curr_dur < *expected_elapsed {
            return false;
        }

        let next_dur = curr_dur + self.backoff;
        let next_micros = next_dur.as_micros() as u64;
        loop {
            match self.next.compare_exchange_weak(
                expected_elapsed.as_micros() as u64,
                next_micros,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    *expected_elapsed = next_dur;
                    return true;
                }
                Err(m) => {
                    *expected_elapsed = Duration::from_micros(m);
                    if curr_dur < *expected_elapsed {
                        return false;
                    }
                }
            }
        }
    }
}

/// Configuration for schedule algorithm.
#[derive(Clone)]
pub(crate) struct SchedConfig {
    /// The maximum number of running threads at the same time.
    pub max_thread_count: usize,
    /// The minimum number of running threads at the same time.
    pub min_thread_count: usize,
    /// The maximum tries to rerun an unfinished task before pushing
    /// back to queue.
    pub max_inplace_spin: usize,
    /// The maximum allowed idle time for a thread. Thread will only be
    /// woken up when algorithm thinks it needs more worker.
    pub max_idle_time: Duration,
    /// The maximum time to wait for a task before increasing the
    /// running thread slots.
    pub max_wait_time: Duration,
    /// The minimum interval between waking a thread.
    pub wake_backoff: Duration,
    /// The minimum interval between increasing running threads.
    pub alloc_slot_backoff: Duration,
}

pub struct WakeManager {
    start_time: Instant,
    wake_backoff: BackOff,
    alloc_slot_backoff: BackOff,
}

impl WakeManager {
    pub fn new(sched_config: &SchedConfig) -> WakeManager {
        WakeManager {
            start_time: Instant::now(),
            wake_backoff: BackOff::new(sched_config.wake_backoff),
            alloc_slot_backoff: BackOff::new(sched_config.alloc_slot_backoff),
        }
    }

    pub fn acquire_slot(&self, time: Instant, expected_elapsed: &mut Duration) -> bool {
        let dur = self.start_time - time;
        !self
            .alloc_slot_backoff
            .should_backoff(dur, expected_elapsed)
    }

    pub fn next_slot_elapsed(&self) -> Duration {
        self.alloc_slot_backoff.next_backoff()
    }

    pub fn acquire_spawn(&self, time: Instant, expected_elapsed: &mut Duration) -> bool {
        let dur = self.start_time - time;
        !self.wake_backoff.should_backoff(dur, expected_elapsed)
    }

    pub fn next_spawn_elapsed(&self) -> Duration {
        self.wake_backoff.next_backoff()
    }
}

struct WakeAgent {
    manager: Arc<WakeManager>,
    next_spawn_cache: Duration,
    next_alloc_slot_cache: Duration,
}

impl WakeAgent {
    fn new(manager: Arc<WakeManager>) -> WakeAgent {
        WakeAgent {
            next_spawn_cache: manager.next_spawn_elapsed(),
            next_alloc_slot_cache: manager.next_slot_elapsed(),
            manager,
        }
    }

    pub fn acquire_slot(&mut self, time: Instant) -> bool {
        self.manager
            .acquire_slot(time, &mut self.next_alloc_slot_cache)
    }

    pub fn acquire_spawn(&mut self, time: Instant) -> bool {
        self.manager.acquire_slot(time, &mut self.next_spawn_cache)
    }
}

pub struct ThreadPoolCore {
    // shutdown bit | available_slots | running_count
    running_info: AtomicUsize,
    manager: Arc<WakeManager>,
    worker_count: usize,
}

impl ThreadPoolCore {
    pub fn new(running_info: AtomicUsize, manager: Arc<WakeManager>, worker_count: usize) -> Self {
        ThreadPoolCore {
            running_info,
            manager,
            worker_count,
        }
    }

    fn sleep(&self, min_slot_count: usize, reduce_slot: bool, pause: bool) -> (usize, bool) {
        let mut running_info = self.running_info.load(Ordering::Acquire);
        loop {
            let mut new_info = running_info;
            if reduce_slot {
                let slot_cnt = slot_count(running_info);
                let running_cnt = running_count(running_info);
                debug_assert!(slot_cnt >= running_cnt);
                if slot_cnt > min_slot_count && slot_cnt > running_cnt {
                    new_info -= SLOTS_BASE
                }
            }
            if pause {
                new_info -= RUNNING_BASE;
            } else if new_info == running_info {
                return (running_info, false);
            }
            if let Err(new_info) = self.running_info.compare_exchange_weak(
                running_info,
                new_info,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                running_info = new_info;
                continue;
            }
            return (new_info, running_info - new_info >= SLOTS_BASE);
        }
    }

    fn wake_up(&self, mut running_info: usize) {
        loop {
            let new_info = running_info + RUNNING_BASE;
            match self.running_info.compare_exchange_weak(
                running_info,
                new_info,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(new_info) => running_info = new_info,
            }
        }
    }

    fn allocate_a_slot(&self, mut last_info: usize) {
        loop {
            let new_info = last_info + SLOTS_BASE;
            match self.running_info.compare_exchange_weak(
                last_info,
                new_info,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(info) => last_info = info,
            }
        }
    }

    fn should_shutdown(&self) -> bool {
        is_shutdown(self.running_info.load(Ordering::SeqCst))
    }

    pub fn shutdown(&self) {
        self.running_info.fetch_or(SHUTDOWN_BIT, Ordering::SeqCst);
    }

    pub fn unpark_one(&self, address: usize, from: usize) -> bool {
        let token = UnparkToken(from);
        let mut to_skipped = usize::max_value();
        let mut running_info = 0;
        let res = unsafe {
            parking_lot_core::unpark_filter(
                address,
                |_| {
                    if to_skipped == usize::max_value() {
                        running_info = self.running_info.load(Ordering::Acquire);
                        if is_slot_full(running_info) {
                            return FilterOp::Stop;
                        }
                        to_skipped = self.worker_count - slot_count(running_info) + 1;
                    }
                    if to_skipped > 1 {
                        to_skipped -= 1;
                        FilterOp::Skip
                    } else if to_skipped == 1 {
                        to_skipped -= 1;
                        self.wake_up(running_info);
                        FilterOp::Unpark
                    } else {
                        FilterOp::Stop
                    }
                },
                |_| token,
            )
        };
        res.unparked_threads > 0
    }

    pub fn unpark_any(&self, check_time: Instant) -> bool {
        let info = self.running_info.load(Ordering::SeqCst);
        if is_shutdown(info) || is_slot_full(info) {
            return false;
        }

        if !is_slot_empty(info) {
            let mut next_spawn_elapsed = self.manager.next_spawn_elapsed();
            if !self
                .manager
                .acquire_spawn(check_time, &mut next_spawn_elapsed)
            {
                return false;
            }
        }

        let address = self as *const ThreadPoolCore as usize;
        self.unpark_one(address, self.worker_count)
    }

    pub fn unpark_shutdown(&self) {
        let address = self as *const ThreadPoolCore as usize;
        unsafe {
            parking_lot_core::unpark_all(address, UnparkToken(self.worker_count + 1));
        }
    }
}

pub struct LocalSpawner<I, L> {
    local_queue: L,
    remote: RemoteSpawner<I>,
}

impl<I, L> LocalSpawn for LocalSpawner<I, L>
where
    I: TaskInjector + Send + Sync,
    L: LocalQueue<TaskCell = I::TaskCell>,
{
    type TaskCell = I::TaskCell;
    type Remote = RemoteSpawner<I>;

    fn spawn(&mut self, task_cell: Self::TaskCell) {
        self.local_queue.push(task_cell);
    }

    fn remote(&self) -> Self::Remote {
        self.remote.clone()
    }
}

pub struct RemoteSpawner<I> {
    injector: I,
}

impl<I> Clone for RemoteSpawner<I>
where
    I: Clone,
{
    fn clone(&self) -> RemoteSpawner<I> {
        RemoteSpawner {
            injector: self.injector.clone(),
        }
    }
}

impl<I> RemoteSpawn for RemoteSpawner<I>
where
    I: TaskInjector + Send + Sync,
{
    type TaskCell = I::TaskCell;

    fn spawn(&self, task_cell: Self::TaskCell) {
        self.injector.push(task_cell);
        // TODO: wake thread
    }
}

pub struct WorkerThread<I, L, R> {
    spawn: LocalSpawner<I, L>,
    runner: R,
    agent: WakeAgent,
    sched_config: SchedConfig,
    core: Arc<ThreadPoolCore>,
    worker_index: usize,
    park_metrics: ParkMetrics,
    dequeue_metrics: DequeueMetrics,
    dequeue_metrics_snap: DequeueMetrics,
}

impl<I, L, R> WorkerThread<I, L, R>
where
    I: TaskInjector + Send + Sync,
    L: LocalQueue<TaskCell = I::TaskCell>,
    R: Runner<Spawn = LocalSpawner<I, L>>,
{
    fn new(
        spawn: LocalSpawner<I, L>,
        runner: R,
        agent: WakeAgent,
        sched_config: SchedConfig,
        core: Arc<ThreadPoolCore>,
        worker_index: usize,
    ) -> Self {
        let park_metrics = ParkMetrics::new(core.worker_count + 1);
        WorkerThread {
            spawn,
            runner,
            agent,
            sched_config,
            core,
            worker_index,
            park_metrics,
            dequeue_metrics: DequeueMetrics::default(),
            dequeue_metrics_snap: DequeueMetrics::default(),
        }
    }

    fn run(mut self) {
        let mut last_spawn_time = Instant::now();
        self.runner.start(&mut self.spawn);

        'out: while !self.core.should_shutdown() {
            match self.spawn.local_queue.pop() {
                Some(Pop {
                    task_cell,
                    schedule_time,
                    from_local,
                }) => {
                    self.dequeue_metrics.hit += 1;
                }
                None => {
                    self.dequeue_metrics.miss += 1;
                }
            }
            let (t, is_local) = match self.queue.deque(&mut self.metrics) {
                (Steal::Success(e), b) => (e, b),
                (Steal::Empty, _) => {
                    let mut tried_times = 0;
                    'inner: loop {
                        match self.queue.deque(&mut self.metrics) {
                            (Steal::Success(e), b) => break 'inner (e, b),
                            (Steal::Empty, _) => tried_times += 1,
                            (Steal::Retry, _) => continue 'out,
                        }
                        if tried_times > self.sched_config.max_inplace_spin {
                            if !self.runner.pause(&mut self.queue) {
                                continue;
                            }
                            match self.park() {
                                (Steal::Retry, _) => {
                                    self.runner.resume(&mut self.queue);
                                    continue 'out;
                                }
                                (Steal::Success(e), b) => {
                                    self.runner.resume(&mut self.queue);
                                    break 'inner (e, b);
                                }
                                (Steal::Empty, _) => {
                                    break 'out;
                                }
                            }
                        }
                    }
                }
                (Steal::Retry, _) => continue,
            };
            let now = Instant::now();
            if elapsed(t.sched_time, now) >= self.sched_config.max_wait_time
                || is_local && elapsed(last_spawn_time, now) >= self.sched_config.spawn_backoff
            {
                let (unparked, allocated) = self.unpark_one(now);
                if unparked {
                    last_spawn_time = now;
                    self.park_metrics.unpark_times += 1;
                }
                self.park_metrics.incr_slots += allocated as usize;
            }
            self.runner.handle(&mut self.queue, t.task, &t.ctx);
        }
        // ctx.dump_metrics();
        self.runner.end(&mut self.queue);
    }

    fn unpark_one(&mut self, check_time: Instant) -> (bool, bool) {
        let running_info = self.core.running_info.load(Ordering::SeqCst);
        if is_shutdown(running_info) {
            return (false, false);
        }
        let running_count = running_count(running_info);
        let slot_count = slot_count(running_info);
        let mut allocated = false;
        if running_count == slot_count {
            if slot_count == self.core.worker_count {
                return (false, false);
            }

            if !self.agent.acquire_spawn(check_time) {
                return (false, false);
            }

            if !self.agent.acquire_slot(check_time) {
                return (false, false);
            }

            allocated = true;
            // println!("{} allocate a slot", self.pos);
            self.core.allocate_a_slot(running_info);
        } else {
            if !self.agent.acquire_spawn(check_time) {
                return (false, allocated);
            }
        }
        debug_assert!(
            running_count <= slot_count,
            "{} {}",
            running_count,
            slot_count
        );

        let address = &*self.core as *const ThreadPoolCore as usize;
        (self.core.unpark_one(address, self.worker_index), allocated)
    }

    fn park(&mut self) -> (Steal<SchedUnit<Q::Task, Q::TaskContext>>, bool) {
        let address = &*self.core as *const ThreadPoolCore as usize;
        let mut deadline = self.get_deadline();
        let mut pause = true;
        let token = ParkToken(self.worker_index);
        let mut task = (Steal::Empty, false);
        loop {
            let res = unsafe {
                parking_lot_core::park(
                    address,
                    || {
                        // running_info should be updated before fetching tasks, as unpark
                        // works in the order of push -> check running info
                        let (running_info, released) = self.core.sleep(
                            self.sched_config.min_thread_count,
                            deadline.is_none(),
                            pause,
                        );
                        // A thread can only be paused once before waken up.
                        pause = false;
                        /*if released {
                            println!("{} release a slot", pos);
                        }*/
                        self.park_metrics.desc_slots += released as usize;
                        if is_shutdown(running_info) {
                            false
                        } else if deadline.is_some() || !is_slot_full(running_info) {
                            task = self.queue.deque(&mut self.metrics);
                            if task.0.is_empty() {
                                true
                            } else {
                                self.core.wake_up(running_info);
                                false
                            }
                        } else {
                            true
                        }
                    },
                    || {},
                    |_, _| (),
                    token,
                    deadline,
                )
            };
            return match res {
                ParkResult::TimedOut => {
                    if deadline.is_some() {
                        deadline = None;
                        continue;
                    } else {
                        (Steal::Empty, false)
                    }
                }
                ParkResult::Invalid => task,
                ParkResult::Unparked(from) => {
                    // println!("{} unpark by {}", self.local_queue.pos, from.0);
                    // If slot has been released, then it must be unparked after the slot
                    // has been allocated; if slot has not been released, it doesn't need
                    // allocate another apparently.
                    if from.0 < self.park_metrics.park_cnt.len() {
                        self.park_metrics.park_cnt[from.0] += 1;
                        (Steal::Retry, false)
                    } else {
                        (Steal::Empty, false)
                    }
                }
            };
        }
    }

    fn get_deadline(&mut self) -> Option<Instant> {
        let miss = self.metrics.miss() - self.metrics_snap.miss();
        let hit = self.metrics.hit() - self.metrics_snap.hit();
        self.metrics_snap = self.metrics.clone();
        if (miss as f64 / (miss + hit) as f64) < self.sched_config.tolerate_miss_rate {
            Some(Instant::now() + self.sched_config.max_idle_time)
        } else {
            None
        }
    }
}

/// A builder for lazy spawning.
pub struct LazyBuilder<I, L> {
    builder: Builder,
    injector: I,
    local_queues: Vec<L>,
}

impl<I, L> LazyBuilder<I, L> {
    /// Sets the name prefix of threads. The thread name will follow the
    /// format "prefix-index".
    pub fn name(mut self, name_prefix: impl Into<String>) -> LazyBuilder<I, L> {
        self.builder.name_prefix = name_prefix.into();
        self
    }
}

impl<I, L> LazyBuilder<I, L>
where
    I: TaskInjector,
    L: LocalQueue + Send + 'static,
{
    /// Spawns all the required threads.
    ///
    /// There will be `max_thread_count` threads spawned. Generally only a few
    /// will keep running in the background, most of them are put to sleep
    /// immediately.
    pub fn build<F>(self, mut factory: F) -> ThreadPool<I>
    where
        F: RunnerBuilder,
        F::Runner: Runner + Send + 'static,
    {
        let mut threads = Vec::with_capacity(self.builder.sched_config.max_thread_count);
        for (i, local_queue) in self.local_queues.into_iter().enumerate() {
            let _r = factory.build();
            let name = format!("{}-{}", self.builder.name_prefix, i);
            let mut builder = thread::Builder::new().name(name);
            if let Some(size) = self.builder.stack_size {
                builder = builder.stack_size(size)
            }
            threads.push(
                builder
                    .spawn(move || {
                        drop(local_queue);
                        unimplemented!()
                    })
                    .unwrap(),
            );
        }
        ThreadPool {
            injector: self.injector,
            threads: Mutex::new(threads),
        }
    }
}

/// A builder for the thread pool.
#[derive(Clone)]
pub struct Builder {
    name_prefix: String,
    stack_size: Option<usize>,
    sched_config: SchedConfig,
}

impl Builder {
    /// Create a builder using the given name prefix.
    pub fn new(name_prefix: impl Into<String>) -> Builder {
        Builder {
            name_prefix: name_prefix.into(),
            stack_size: None,
            sched_config: SchedConfig {
                max_thread_count: num_cpus::get(),
                min_thread_count: 1,
                max_inplace_spin: 4,
                max_idle_time: Duration::from_millis(1),
                max_wait_time: Duration::from_millis(1),
                wake_backoff: Duration::from_millis(1),
                alloc_slot_backoff: Duration::from_millis(2),
            },
        }
    }

    /// Sets the maximum number of running threads at the same time.
    pub fn max_thread_count(&mut self, count: usize) -> &mut Self {
        if count > 0 {
            self.sched_config.max_thread_count = count;
        }
        self
    }

    /// Sets the minimum number of running threads at the same time.
    pub fn min_thread_count(&mut self, count: usize) -> &mut Self {
        if count > 0 {
            self.sched_config.min_thread_count = count;
        }
        self
    }

    /// Sets the maximum tries to rerun an unfinished task before pushing
    /// back to queue.
    pub fn max_inplace_spin(&mut self, count: usize) -> &mut Self {
        self.sched_config.max_inplace_spin = count;
        self
    }

    /// Sets the maximum allowed idle time for a thread. Thread will only be
    /// woken up when algorithm thinks it needs more worker.
    pub fn max_idle_time(&mut self, time: Duration) -> &mut Self {
        self.sched_config.max_idle_time = time;
        self
    }

    /// Sets the maximum time to wait for a task before increasing the
    /// running thread slots.
    pub fn max_wait_time(&mut self, time: Duration) -> &mut Self {
        self.sched_config.max_wait_time = time;
        self
    }

    /// Sets the minimum interval between waking a thread.
    pub fn wake_backoff(&mut self, time: Duration) -> &mut Self {
        self.sched_config.wake_backoff = time;
        self
    }

    /// Sets the minimum interval between increasing running threads.
    pub fn alloc_slot_backoff(&mut self, time: Duration) -> &mut Self {
        self.sched_config.alloc_slot_backoff = time;
        self
    }

    /// Sets the stack size of the spawned threads.
    pub fn stack_size(&mut self, size: usize) -> &mut Self {
        if size > 0 {
            self.stack_size = Some(size);
        }
        self
    }

    /// Freezes the configurations and returns the task scheduler and
    /// a builder to for lazy spawning threads.
    ///
    /// `queue_builder` is a closure that creates a task queue. It accepts the
    /// number of local queues and returns the task injector and local queues.
    ///
    /// In some cases, especially building up a large application, a task
    /// scheduler is required before spawning new threads. You can use this
    /// to separate the construction and starting.
    pub fn freeze<I, L>(
        &self,
        queue_builder: impl FnOnce(usize) -> (I, Vec<L>),
    ) -> (Remote<I>, LazyBuilder<I, L>)
    where
        I: TaskInjector,
        L: LocalQueue<TaskCell = I::TaskCell> + Send + 'static,
    {
        assert!(self.sched_config.min_thread_count <= self.sched_config.max_thread_count);
        let (injector, local_queues) = queue_builder(self.sched_config.max_thread_count);

        (
            Remote {
                injector: injector.clone(),
            },
            LazyBuilder {
                builder: self.clone(),
                injector,
                local_queues,
            },
        )
    }

    /// Spawns the thread pool immediately.
    ///
    /// `queue_builder` is a closure that creates a task queue. It accepts the
    /// number of local queues and returns the task injector and local queues.
    pub fn build<I, L, B>(
        &self,
        queue_builder: impl FnOnce(usize) -> (I, Vec<L>),
        runner_builder: B,
    ) -> ThreadPool<I>
    where
        I: TaskInjector,
        L: LocalQueue<TaskCell = I::TaskCell> + Send + 'static,
        B: RunnerBuilder,
        B::Runner: Runner + Send + 'static,
    {
        self.freeze(queue_builder).1.build(runner_builder)
    }
}

/// A generic thread pool.
pub struct ThreadPool<I: TaskInjector> {
    injector: I,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<I: TaskInjector> ThreadPool<I> {
    /// Spawns the task into the thread pool.
    ///
    /// If the pool is shutdown, it becomes no-op.
    pub fn spawn(&self, t: impl Into<I::TaskCell>) {
        self.injector.push(t.into());
    }

    /// Shutdowns the pool.
    ///
    /// Closes the queue and wait for all threads to exit.
    pub fn shutdown(&self) {
        let mut threads = mem::replace(&mut *self.threads.lock().unwrap(), Vec::new());
        for j in threads.drain(..) {
            j.join().unwrap();
        }
    }

    /// Get a remote queue for spawning tasks without owning the thread pool.
    pub fn remote(&self) -> Remote<I> {
        Remote {
            injector: self.injector.clone(),
        }
    }
}

impl<I: TaskInjector> Drop for ThreadPool<I> {
    /// Will shutdown the thread pool if it has not.
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A remote handle that can spawn tasks to the thread pool without owning
/// it.
#[derive(Clone)]
pub struct Remote<I> {
    injector: I,
}

impl<I: TaskInjector> Remote<I> {
    /// Spawns the tasks into thread pool.
    ///
    /// If the thread pool is shutdown, it becomes no-op.
    pub fn spawn(&self, _t: impl Into<I::TaskCell>) {
        unimplemented!()
    }
}
