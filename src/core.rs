// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{LocalSpawn, RemoteSpawn, Runner};

use crossbeam_deque::Steal;
use parking_lot_core::{FilterOp, ParkResult, ParkToken, UnparkToken};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};
use std::{mem, usize};

const SHUTDOWN_BIT: usize = 0x01;
const RUNNING_BASE_SHIFT: usize = 1;
const RUNNING_BASE: usize = 1 << RUNNING_BASE_SHIFT;
const SLOTS_BASE_SHIFT: usize = 16;
const SLOTS_BASE: usize = 1 << SLOTS_BASE_SHIFT;

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

pub struct SchedUnit<Task> {
    task: Task,
    sched_time: Instant,
}

impl<Task> SchedUnit<Task> {
    fn new(task: Task) -> SchedUnit<Task> {
        SchedUnit {
            task,
            sched_time: Instant::now(),
        }
    }
}

#[derive(Clone, Default, Debug)]
struct HandleMetrics {
    handled_global: usize,
    handled_miss: usize,
    handled_local: usize,
    handled_steal: usize,
    handled_retry: usize,
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

struct SpawnManager {
    start_time: Instant,
    spawn_backoff: BackOff,
    alloc_slot_backoff: BackOff,
}

impl SpawnManager {
    fn new(sched_config: &SchedConfig) -> SpawnManager {
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

pub trait LocalQueue {
    type Task;

    fn pop(&mut self) -> Option<Self::Task>;
}

pub trait RemoteQueue {
    type Task;
    type Local: LocalQueue<Task = Self::Task>;

    fn steal(&self, local: &mut Self::Local) -> bool;

    fn steal_batch(&self, local: &mut Self::Local) -> bool {
        self.steal(local)
    }

    fn steal_batch_and_pop(&self, local: &mut Self::Local) -> Option<Self::Task> {
        self.steal_batch(local);
        local.pop()
    }
}

// pub struct PoolContext<G: GlobalQueue> {
//     local_queue: LocalQueue<G>,
//     metrics: HandleMetrics,
//     park_metrics: ParkMetrics,
//     agent: SpawnAgent,
//     spawn_local: usize,
//     metrics_snap: HandleMetrics,
// }

// impl<G: GlobalQueue> PoolContext<G> {
//     fn new(queue: LocalQueue<G>, agent: SpawnAgent) -> PoolContext<G> {
//         PoolContext {
//             metrics: HandleMetrics::default(),
//             spawn_local: 0,
//             agent,
//             park_metrics: ParkMetrics::new(queue.core.stealers.len() + 1),
//             local_queue: queue,
//             metrics_snap: HandleMetrics::default(),
//         }
//     }

//     pub fn spawn(&mut self, t: impl Into<G::Task>) {
//         self.spawn_local += 1;
//         self.local_queue.local.push(SchedUnit::new(t.into()));
//     }

//     pub fn remote(&self) -> Remote<G> {
//         Remote {
//             queue: Queues {
//                 core: self.local_queue.core.clone(),
//             },
//         }
//     }

//     pub fn global_queue(&self) -> &G {
//         &self.local_queue.core.global
//     }

//     #[inline]
//     fn deque_a_task(&mut self) -> (Steal<SchedUnit<G::Task>>, bool) {
//         if let Some(e) = self.local_queue.local.pop() {
//             self.metrics.handled_local += 1;
//             return (Steal::Success(e), true);
//         }
//         self.deque_global_task()
//     }

//     fn deque_global_task(&mut self) -> (Steal<SchedUnit<G::Task>>, bool) {
//         let mut need_retry = false;
//         match self
//             .local_queue
//             .core
//             .global
//             .steal_batch_and_pop(&self.local_queue.local)
//         {
//             e @ Steal::Success(_) => {
//                 self.metrics.handled_global += 1;
//                 return (e, false);
//             }
//             Steal::Retry => need_retry = true,
//             _ => {}
//         }
//         for (pos, stealer) in self.local_queue.core.stealers.iter().enumerate() {
//             if pos != self.local_queue.pos {
//                 match stealer.steal_batch_and_pop(&self.local_queue.local) {
//                     e @ Steal::Success(_) => {
//                         self.metrics.handled_steal += 1;
//                         return (e, false);
//                     }
//                     Steal::Retry => need_retry = true,
//                     _ => {}
//                 }
//             }
//         }
//         if need_retry {
//             self.metrics.handled_retry += 1;
//             (Steal::Retry, false)
//         } else {
//             self.metrics.handled_miss += 1;
//             (Steal::Empty, false)
//         }
//     }

//     fn get_deadline(&mut self, sched_config: &SchedConfig) -> Option<Instant> {
//         let miss = self.metrics.miss() - self.metrics_snap.miss();
//         let hit = self.metrics.hit() - self.metrics_snap.hit();
//         self.metrics_snap = self.metrics.clone();
//         if (miss as f64 / (miss + hit) as f64) < sched_config.tolerate_miss_rate {
//             Some(Instant::now() + sched_config.max_idle_time)
//         } else {
//             None
//         }
//     }

//     fn park(&mut self, sched_config: &SchedConfig) -> (Steal<SchedUnit<G::Task>>, bool) {
//         let address = &*self.local_queue.core as *const QueueCore<G> as usize;
//         let mut deadline = self.get_deadline(sched_config);
//         let mut pause = true;
//         let token = ParkToken(self.local_queue.pos);
//         let mut task = (Steal::Empty, false);
//         // let pos = self.local_queue.pos;
//         loop {
//             let res = unsafe {
//                 parking_lot_core::park(
//                     address,
//                     || {
//                         // running_info should be updated before fetching tasks, as unpark
//                         // works in the order of push -> check running info
//                         let (running_info, released) = self.local_queue.core.sleep(
//                             sched_config.min_thread_count,
//                             deadline.is_none(),
//                             pause,
//                         );
//                         // A thread can only be paused once before waken up.
//                         pause = false;
//                         /*if released {
//                             println!("{} release a slot", pos);
//                         }*/
//                         self.park_metrics.desc_slots += released as usize;
//                         if is_shutdown(running_info) {
//                             false
//                         } else if deadline.is_some() || !is_slot_full(running_info) {
//                             task = self.deque_a_task();
//                             if task.0.is_empty() {
//                                 true
//                             } else {
//                                 self.local_queue.core.wake_up(running_info);
//                                 false
//                             }
//                         } else {
//                             true
//                         }
//                     },
//                     || {},
//                     |_, _| (),
//                     token,
//                     deadline,
//                 )
//             };
//             return match res {
//                 ParkResult::TimedOut => {
//                     if deadline.is_some() {
//                         deadline = None;
//                         continue;
//                     } else {
//                         (Steal::Empty, false)
//                     }
//                 }
//                 ParkResult::Invalid => task,
//                 ParkResult::Unparked(from) => {
//                     // println!("{} unpark by {}", self.local_queue.pos, from.0);
//                     // If slot has been released, then it must be unparked after the slot
//                     // has been allocated; if slot has not been released, it doesn't need
//                     // allocate another apparently.
//                     if from.0 < self.park_metrics.park_cnt.len() {
//                         self.park_metrics.park_cnt[from.0] += 1;
//                         (Steal::Retry, false)
//                     } else {
//                         (Steal::Empty, false)
//                     }
//                 }
//             };
//         }
//     }

//     fn dump_metrics(&self) {
//         println!(
//             "{} park {:?} {:?}",
//             self.local_queue.pos, self.park_metrics, self.metrics,
//         );
//     }
// }

// pub struct Remote<G: GlobalQueue> {
//     queue: Queues<G>,
// }

// impl<G: GlobalQueue> Clone for Remote<G> {
//     fn clone(&self) -> Remote<G> {
//         Remote {
//             queue: self.queue.clone(),
//         }
//     }
// }

// impl<G: GlobalQueue> Remote<G> {
//     pub fn spawn(&self, t: impl Into<G::Task>) {
//         self.queue.push(t.into());
//     }
// }

// pub trait Runner {
//     type GlobalQueue: GlobalQueue;

//     fn start(&mut self, _ctx: &mut PoolContext<Self::GlobalQueue>) {}
//     fn handle(
//         &mut self,
//         ctx: &mut PoolContext<Self::GlobalQueue>,
//         task: <Self::GlobalQueue as GlobalQueue>::Task,
//     ) -> bool;
//     fn pause(&mut self, _ctx: &PoolContext<Self::GlobalQueue>) -> bool {
//         true
//     }
//     fn resume(&mut self, _ctx: &PoolContext<Self::GlobalQueue>) {}
//     fn end(&mut self, _ctx: &PoolContext<Self::GlobalQueue>) {}
// }

// pub trait RunnerFactory {
//     type Runner: Runner;

//     fn produce(&mut self) -> Self::Runner;
// }

// struct QueueCore<G: GlobalQueue> {
//     global: G,
//     stealers: Vec<crossbeam_deque::Stealer<SchedUnit<G::Task>>>,
//     manager: Arc<SpawnManager>,
//     // shutdown bit | available_slots | running_count
//     running_info: AtomicUsize,
//     locals: Mutex<Vec<Option<crossbeam_deque::Worker<SchedUnit<G::Task>>>>>,
// }

// impl<G: GlobalQueue> QueueCore<G> {
//     fn sleep(&self, min_slot_count: usize, reduce_slot: bool, pause: bool) -> (usize, bool) {
//         let mut running_info = self.running_info.load(Ordering::Acquire);
//         loop {
//             let mut new_info = running_info;
//             if reduce_slot {
//                 let slot_cnt = slot_count(running_info);
//                 let running_cnt = running_count(running_info);
//                 debug_assert!(slot_cnt >= running_cnt);
//                 if slot_cnt > min_slot_count && slot_cnt > running_cnt {
//                     new_info -= SLOTS_BASE
//                 }
//             }
//             if pause {
//                 new_info -= RUNNING_BASE;
//             } else if new_info == running_info {
//                 return (running_info, false);
//             }
//             if let Err(new_info) = self.running_info.compare_exchange_weak(
//                 running_info,
//                 new_info,
//                 Ordering::AcqRel,
//                 Ordering::Acquire,
//             ) {
//                 running_info = new_info;
//                 continue;
//             }
//             return (new_info, running_info - new_info >= SLOTS_BASE);
//         }
//     }

//     fn wake_up(&self, mut running_info: usize) {
//         loop {
//             let new_info = running_info + RUNNING_BASE;
//             match self.running_info.compare_exchange_weak(
//                 running_info,
//                 new_info,
//                 Ordering::AcqRel,
//                 Ordering::Acquire,
//             ) {
//                 Ok(_) => return,
//                 Err(new_info) => running_info = new_info,
//             }
//         }
//     }

//     fn allocate_a_slot(&self, mut last_info: usize) {
//         loop {
//             let new_info = last_info + SLOTS_BASE;
//             match self.running_info.compare_exchange_weak(
//                 last_info,
//                 new_info,
//                 Ordering::AcqRel,
//                 Ordering::Acquire,
//             ) {
//                 Ok(_) => return,
//                 Err(info) => last_info = info,
//             }
//         }
//     }

//     fn should_shutdown(&self) -> bool {
//         is_shutdown(self.running_info.load(Ordering::SeqCst))
//     }

//     fn shutdown(&self) {
//         self.running_info.fetch_or(SHUTDOWN_BIT, Ordering::SeqCst);
//     }

//     fn unpark_one(&self, address: usize, from: usize) -> bool {
//         let token = UnparkToken(from);
//         let mut to_skipped = usize::MAX;
//         let mut running_info = 0;
//         let res = unsafe {
//             parking_lot_core::unpark_filter(
//                 address,
//                 |_| {
//                     if to_skipped == usize::MAX {
//                         running_info = self.running_info.load(Ordering::Acquire);
//                         if is_slot_full(running_info) {
//                             return FilterOp::Stop;
//                         }
//                         to_skipped = self.stealers.len() - slot_count(running_info) + 1;
//                     }
//                     if to_skipped > 1 {
//                         to_skipped -= 1;
//                         FilterOp::Skip
//                     } else if to_skipped == 1 {
//                         to_skipped -= 1;
//                         self.wake_up(running_info);
//                         FilterOp::Unpark
//                     } else {
//                         FilterOp::Stop
//                     }
//                 },
//                 |_| token,
//             )
//         };
//         res.unparked_threads > 0
//     }
// }

// struct LocalQueue<G: GlobalQueue> {
//     local: crossbeam_deque::Worker<SchedUnit<G::Task>>,
//     core: Arc<QueueCore<G>>,
//     pos: usize,
// }

// impl<G: GlobalQueue> LocalQueue<G> {
//     fn unpark_one(&mut self, agent: &mut SpawnAgent, check_time: Instant) -> (bool, bool) {
//         let running_info = self.core.running_info.load(Ordering::SeqCst);
//         if is_shutdown(running_info) {
//             return (false, false);
//         }
//         let running_count = running_count(running_info);
//         let slot_count = slot_count(running_info);
//         let mut allocated = false;
//         if running_count == slot_count {
//             if slot_count == self.core.stealers.len() {
//                 return (false, false);
//             }

//             if !agent.acquire_spawn(check_time) {
//                 return (false, false);
//             }

//             if !agent.acquire_slot(check_time) {
//                 return (false, false);
//             }

//             allocated = true;
//             // println!("{} allocate a slot", self.pos);
//             self.core.allocate_a_slot(running_info);
//         } else {
//             if !agent.acquire_spawn(check_time) {
//                 return (false, allocated);
//             }
//         }
//         debug_assert!(
//             running_count <= slot_count,
//             "{} {}",
//             running_count,
//             slot_count
//         );

//         let address = &*self.core as *const QueueCore<G> as usize;
//         (self.core.unpark_one(address, self.pos), allocated)
//     }
// }

// struct Queues<G: GlobalQueue> {
//     core: Arc<QueueCore<G>>,
// }

// impl<G: GlobalQueue> Clone for Queues<G> {
//     fn clone(&self) -> Queues<G> {
//         Queues {
//             core: self.core.clone(),
//         }
//     }
// }

// impl<G: GlobalQueue> Queues<G> {
//     fn acquire_local_queue(&self) -> LocalQueue<G> {
//         let mut locals = self.core.locals.lock().unwrap();
//         for (pos, l) in locals.iter_mut().enumerate() {
//             if l.is_some() {
//                 return LocalQueue {
//                     local: l.take().unwrap(),
//                     core: self.core.clone(),
//                     pos,
//                 };
//             }
//         }
//         unreachable!()
//     }

//     fn release_local_queue(&mut self, q: LocalQueue<G>) {
//         let mut locals = self.core.locals.lock().unwrap();
//         assert!(locals[q.pos].replace(q.local).is_none());
//     }

//     fn unpark_one(&self, check_time: Instant) -> bool {
//         let info = self.core.running_info.load(Ordering::SeqCst);
//         if is_shutdown(info) || is_slot_full(info) {
//             return false;
//         }

//         if !is_slot_empty(info) {
//             let mut next_spawn_elapsed = self.core.manager.next_spawn_elapsed();
//             if !self
//                 .core
//                 .manager
//                 .acquire_spawn(check_time, &mut next_spawn_elapsed)
//             {
//                 return false;
//             }
//         }

//         let address = &*self.core as *const QueueCore<G> as usize;
//         self.core.unpark_one(address, self.core.stealers.len())
//     }

//     fn unpark_shutdown(&self) {
//         let address = &*self.core as *const QueueCore<G> as usize;
//         unsafe {
//             parking_lot_core::unpark_all(address, UnparkToken(self.core.stealers.len() + 1));
//         }
//     }

//     fn push(&self, task: G::Task) {
//         let u = SchedUnit::new(task);
//         let sched_time = u.sched_time;
//         self.core.global.push(u);
//         self.unpark_one(sched_time);
//     }
// }

fn elapsed(start: Instant, end: Instant) -> Duration {
    if start < end {
        end.duration_since(start)
    } else {
        Duration::from_secs(0)
    }
}

// pub struct WorkerThread<G, R>
// where
//     G: GlobalQueue,
//     R: Runner<GlobalQueue = G>,
// {
//     local: LocalQueue<G>,
//     runner: R,
//     agent: SpawnAgent,
//     sched_config: SchedConfig,
// }

// impl<G, R> WorkerThread<G, R>
// where
//     G: GlobalQueue,
//     R: Runner<GlobalQueue = G>,
// {
//     fn new(local: LocalQueue<G>, agent: SpawnAgent, runner: R, sched_config: SchedConfig) -> Self {
//         WorkerThread {
//             local,
//             agent,
//             runner,
//             sched_config,
//         }
//     }

//     fn run(mut self) {
//         let mut ctx = PoolContext::new(self.local, self.agent);
//         let mut last_spawn_time = Instant::now();
//         self.runner.start(&mut ctx);

//         'out: while !ctx.local_queue.core.should_shutdown() {
//             let (t, is_local) = match ctx.deque_a_task() {
//                 (Steal::Success(e), b) => (e, b),
//                 (Steal::Empty, _) => {
//                     let mut tried_times = 0;
//                     'inner: loop {
//                         match ctx.deque_a_task() {
//                             (Steal::Success(e), b) => break 'inner (e, b),
//                             (Steal::Empty, _) => tried_times += 1,
//                             (Steal::Retry, _) => continue 'out,
//                         }
//                         if tried_times > self.sched_config.max_inplace_spin {
//                             if !self.runner.pause(&ctx) {
//                                 continue;
//                             }
//                             match ctx.park(&self.sched_config) {
//                                 (Steal::Retry, _) => {
//                                     self.runner.resume(&ctx);
//                                     continue 'out;
//                                 }
//                                 (Steal::Success(e), b) => {
//                                     self.runner.resume(&ctx);
//                                     break 'inner (e, b);
//                                 }
//                                 (Steal::Empty, _) => {
//                                     break 'out;
//                                 }
//                             }
//                         }
//                     }
//                 }
//                 (Steal::Retry, _) => continue,
//             };
//             let now = Instant::now();
//             if elapsed(t.sched_time, now) >= self.sched_config.max_wait_time
//                 || is_local && elapsed(last_spawn_time, now) >= self.sched_config.spawn_backoff
//             {
//                 let (unparked, allocated) = ctx.local_queue.unpark_one(&mut ctx.agent, now);
//                 if unparked {
//                     last_spawn_time = now;
//                     ctx.park_metrics.unpark_times += 1;
//                 }
//                 ctx.park_metrics.incr_slots += allocated as usize;
//             }
//             self.runner.handle(&mut ctx, t.task);
//         }
//         // ctx.dump_metrics();
//         self.runner.end(&ctx);
//     }
// }

#[derive(Clone)]
struct SchedConfig {
    max_thread_count: usize,
    min_thread_count: usize,
    max_inplace_spin: usize,
    max_idle_time: Duration,
    tolerate_miss_rate: f64,
    max_wait_time: Duration,
    spawn_backoff: Duration,
    alloc_slot_backoff: Duration,
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

// #[derive(Clone)]
// pub struct Config {
//     name_prefix: String,
//     stack_size: Option<usize>,
//     sched_config: SchedConfig,
// }

// impl Config {
//     pub fn new(name_prefix: impl Into<String>) -> Config {
//         Config {
//             name_prefix: name_prefix.into(),
//             stack_size: None,
//             sched_config: SchedConfig {
//                 max_thread_count: num_cpus::get(),
//                 min_thread_count: 1,
//                 max_inplace_spin: 4,
//                 max_idle_time: Duration::from_millis(1),
//                 // In general, when a thread is waken up to handle tasks,
//                 // it can handle at least one task, then the waken up makes sence.
//                 // After a thread handles a request, it will sleep again when there
//                 // are three continous misses. So the miss rate of a reasonalbe woken up
//                 // should not be higher than 3 / 4.
//                 tolerate_miss_rate: 3.0 / 4.0,
//                 max_wait_time: Duration::from_millis(1),
//                 spawn_backoff: Duration::from_millis(1),
//                 alloc_slot_backoff: Duration::from_millis(2),
//             },
//         }
//     }

//     pub fn max_thread_count(&mut self, count: usize) -> &mut Config {
//         if count > 0 {
//             self.sched_config.max_thread_count = count;
//         }
//         self
//     }

//     pub fn min_thread_count(&mut self, count: usize) -> &mut Config {
//         if count > 0 {
//             self.sched_config.min_thread_count = count;
//         }
//         self
//     }

//     pub fn max_inplace_spin(&mut self, count: usize) -> &mut Config {
//         self.sched_config.max_inplace_spin = count;
//         self
//     }

//     pub fn max_idle_time(&mut self, time: Duration) -> &mut Config {
//         self.sched_config.max_idle_time = time;
//         self
//     }

//     pub fn max_wait_time(&mut self, time: Duration) -> &mut Config {
//         self.sched_config.max_wait_time = time;
//         self
//     }

//     pub fn spawn_backoff(&mut self, time: Duration) -> &mut Config {
//         self.sched_config.spawn_backoff = time;
//         self
//     }

//     pub fn alloc_slot_backoff(&mut self, time: Duration) -> &mut Config {
//         self.sched_config.alloc_slot_backoff = time;
//         self
//     }

//     pub fn stack_size(&mut self, size: usize) -> &mut Config {
//         if size > 0 {
//             self.stack_size = Some(size);
//         }
//         self
//     }

//     pub fn freeze<G: GlobalQueue>(
//         &self,
//         global_ctor: impl FnOnce() -> G,
//     ) -> (Remote<G>, LazyConfig<G>) {
//         let global = global_ctor();
//         assert!(self.sched_config.max_thread_count < SLOTS_BASE >> RUNNING_BASE_SHIFT);
//         assert!(self.sched_config.min_thread_count <= self.sched_config.max_thread_count);
//         let mut workers = Vec::with_capacity(self.sched_config.max_thread_count);
//         let mut stealers = Vec::with_capacity(self.sched_config.max_thread_count);
//         for _ in 0..self.sched_config.max_thread_count {
//             let w = crossbeam_deque::Worker::new_lifo();
//             stealers.push(w.stealer());
//             workers.push(Some(w));
//         }
//         let running_info = (self.sched_config.max_thread_count << SLOTS_BASE_SHIFT)
//             | (self.sched_config.max_thread_count << RUNNING_BASE_SHIFT);
//         let manager = Arc::new(SpawnManager::new(&self.sched_config));
//         let queues = Queues {
//             core: Arc::new(QueueCore {
//                 global,
//                 stealers,
//                 manager,
//                 locals: Mutex::new(workers),
//                 running_info: AtomicUsize::new(running_info),
//             }),
//         };
//         (
//             Remote {
//                 queue: queues.clone(),
//             },
//             LazyConfig {
//                 cfg: self.clone(),
//                 queues,
//             },
//         )
//     }

//     pub fn spawn<F>(
//         &self,
//         factory: F,
//         global_ctor: impl FnOnce() -> <F::Runner as Runner>::GlobalQueue,
//     ) -> ThreadPool<<F::Runner as Runner>::GlobalQueue>
//     where
//         F: RunnerFactory,
//         F::Runner: Send + 'static,
//         <F::Runner as Runner>::GlobalQueue: Send + Sync + 'static,
//         <<F::Runner as Runner>::GlobalQueue as GlobalQueue>::Task: Send + 'static,
//     {
//         self.freeze(global_ctor).1.spawn(factory)
//     }
// }

// pub struct ThreadPoolCore<G: GlobalQueue> {
//     queues: Queues<G>,
//     threads: Mutex<Vec<JoinHandle<()>>>,
// }

// impl<G: GlobalQueue> ThreadPool<G> {
//     pub fn spawn(&self, t: impl Into<G::Task>) {
//         self.queues.push(t.into());
//     }

//     pub fn remote(&self) -> Remote<G> {
//         Remote {
//             queue: self.queues.clone(),
//         }
//     }

//     pub fn global_queue(&self) -> &G {
//         &self.queues.core.global
//     }

//     pub fn shutdown(&self) {
//         self.queues.core.shutdown();
//         let mut threads = mem::replace(&mut *self.threads.lock().unwrap(), Vec::new());
//         for _ in 0..threads.len() {
//             self.queues.unpark_shutdown();
//         }
//         for j in threads.drain(..) {
//             j.join().unwrap();
//         }
//     }
// }

// impl<G: GlobalQueue> Drop for ThreadPool<G> {
//     fn drop(&mut self) {
//         self.shutdown();
//     }
// }
