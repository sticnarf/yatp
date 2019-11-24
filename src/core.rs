// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{LocalSpawn, Queue, RemoteSpawn, Runner};

use crossbeam_deque::Steal;
use parking_lot_core::{FilterOp, ParkResult, ParkToken, UnparkToken};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};
use std::{mem, usize};

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

#[inline]
fn elapsed(start: Instant, end: Instant) -> Duration {
    if start < end {
        end.duration_since(start)
    } else {
        Duration::from_secs(0)
    }
}

pub struct SchedUnit<Task, TaskContext> {
    task: Task,
    ctx: TaskContext,
    sched_time: Instant,
}

impl<Task, TaskContext> SchedUnit<Task, TaskContext> {
    pub fn new(task: Task, ctx: TaskContext) -> SchedUnit<Task, TaskContext> {
        SchedUnit {
            task,
            ctx,
            sched_time: Instant::now(),
        }
    }

    pub fn sched_time(&self) -> Instant {
        self.sched_time
    }
}

#[derive(Clone, Default, Debug)]
pub struct HandleMetrics {
    pub handled_global: usize,
    pub handled_miss: usize,
    pub handled_local: usize,
    pub handled_steal: usize,
    pub handled_retry: usize,
}

impl HandleMetrics {
    fn miss(&self) -> usize {
        self.handled_miss
    }

    fn hit(&self) -> usize {
        self.handled_global + self.handled_local + self.handled_steal
    }
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

#[derive(Clone)]
pub struct SchedConfig {
    pub max_thread_count: usize,
    pub min_thread_count: usize,
    pub max_inplace_spin: usize,
    pub max_idle_time: Duration,
    pub tolerate_miss_rate: f64,
    pub max_wait_time: Duration,
    pub spawn_backoff: Duration,
    pub alloc_slot_backoff: Duration,
}

pub struct SpawnManager {
    start_time: Instant,
    spawn_backoff: BackOff,
    alloc_slot_backoff: BackOff,
}

impl SpawnManager {
    pub fn new(sched_config: &SchedConfig) -> SpawnManager {
        SpawnManager {
            start_time: Instant::now(),
            spawn_backoff: BackOff::new(sched_config.spawn_backoff),
            alloc_slot_backoff: BackOff::new(sched_config.alloc_slot_backoff),
        }
    }

    pub fn acquire_slot(&self, time: Instant, expected_elapsed: &mut Duration) -> bool {
        let dur = elapsed(self.start_time, time);
        !self
            .alloc_slot_backoff
            .should_backoff(dur, expected_elapsed)
    }

    pub fn next_slot_elapsed(&self) -> Duration {
        self.alloc_slot_backoff.next_backoff()
    }

    pub fn acquire_spawn(&self, time: Instant, expected_elapsed: &mut Duration) -> bool {
        let dur = elapsed(self.start_time, time);
        !self.spawn_backoff.should_backoff(dur, expected_elapsed)
    }

    pub fn next_spawn_elapsed(&self) -> Duration {
        self.spawn_backoff.next_backoff()
    }
}

struct SpawnAgent {
    manager: Arc<SpawnManager>,
    next_spawn_cache: Duration,
    next_alloc_slot_cache: Duration,
}

impl SpawnAgent {
    fn new(manager: Arc<SpawnManager>) -> SpawnAgent {
        SpawnAgent {
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
    manager: Arc<SpawnManager>,
    worker_count: usize,
}

impl ThreadPoolCore {
    pub fn new(running_info: AtomicUsize, manager: Arc<SpawnManager>, worker_count: usize) -> Self {
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
        let mut to_skipped = usize::MAX;
        let mut running_info = 0;
        let res = unsafe {
            parking_lot_core::unpark_filter(
                address,
                |_| {
                    if to_skipped == usize::MAX {
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

pub struct WorkerThread<Q, R> {
    queue: Q,
    runner: R,
    agent: SpawnAgent,
    sched_config: SchedConfig,
    core: Arc<ThreadPoolCore>,
    worker_index: usize,
    metrics: HandleMetrics,
    park_metrics: ParkMetrics,
    metrics_snap: HandleMetrics,
}

impl<Q, R> WorkerThread<Q, R>
where
    Q: Queue,
    R: Runner<Task = Q::Task, Spawn = Q>,
{
    fn new(
        queue: Q,
        runner: R,
        agent: SpawnAgent,
        sched_config: SchedConfig,
        core: Arc<ThreadPoolCore>,
        worker_index: usize,
    ) -> Self {
        let park_metrics = ParkMetrics::new(core.worker_count + 1);
        WorkerThread {
            queue,
            runner,
            agent,
            sched_config,
            core,
            worker_index,
            metrics: HandleMetrics::default(),
            park_metrics,
            metrics_snap: HandleMetrics::default(),
        }
    }

    fn run(mut self) {
        let mut last_spawn_time = Instant::now();
        self.runner.start(&mut self.queue);

        'out: while !self.core.should_shutdown() {
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

// pub struct LazyConfig<G: GlobalQueue> {
//     cfg: Config,
//     queues: Queues<G>,
// }

// impl<G: GlobalQueue> LazyConfig<G> {
//     pub fn name(mut self, name_prefix: impl Into<String>) -> LazyConfig<G> {
//         self.cfg.name_prefix = name_prefix.into();
//         self
//     }

//     pub fn spawn<F>(self, mut factory: F) -> ThreadPool<G>
//     where
//         G: GlobalQueue + Send + Sync + 'static,
//         G::Task: Send + 'static,
//         F: RunnerFactory,
//         F::Runner: Runner<GlobalQueue = G> + Send + 'static,
//     {
//         let mut threads = Vec::with_capacity(self.cfg.sched_config.max_thread_count);
//         for i in 0..self.cfg.sched_config.max_thread_count {
//             let r = factory.produce();
//             let local_queue = self.queues.acquire_local_queue();
//             let agent = SpawnAgent::new(self.queues.core.manager.clone());
//             let th = WorkerThread::new(local_queue, agent, r, self.cfg.sched_config.clone());
//             let mut builder = Builder::new().name(format!("{}-{}", self.cfg.name_prefix, i));
//             if let Some(size) = self.cfg.stack_size {
//                 builder = builder.stack_size(size)
//             }
//             threads.push(builder.spawn(move || th.run()).unwrap());
//         }
//         ThreadPool {
//             queues: self.queues,
//             threads: Mutex::new(threads),
//         }
//     }
// }
