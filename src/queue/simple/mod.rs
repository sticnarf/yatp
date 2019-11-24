use crate::core::{HandleMetrics, ThreadPoolCore};
use crate::{LocalSpawn, Queue, RemoteSpawn};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::sync::Arc;

type SchedUnit<Task> = crate::core::SchedUnit<Task, ()>;

pub struct SimpleQueue<Task> {
    local: Worker<SchedUnit<Task>>,
    stealers: Vec<Stealer<SchedUnit<Task>>>,
    remote: SimpleQueueRemote<Task>,
}

impl<Task: Send> LocalSpawn for SimpleQueue<Task> {
    type Task = Task;
    type TaskContext = ();
    type Remote = SimpleQueueRemote<Task>;

    fn spawn_ctx(&mut self, t: impl Into<Self::Task>, _ctx: &()) {
        self.local.push(SchedUnit::new(t.into(), ()));
    }

    fn remote(&self) -> Self::Remote {
        self.remote.clone()
    }
}

pub struct SimpleQueueRemote<Task> {
    injector: Arc<Injector<SchedUnit<Task>>>,
    core: Arc<ThreadPoolCore>,
}

impl<Task> Clone for SimpleQueueRemote<Task> {
    fn clone(&self) -> Self {
        SimpleQueueRemote {
            injector: self.injector.clone(),
            core: self.core.clone(),
        }
    }
}

impl<Task: Send> RemoteSpawn for SimpleQueueRemote<Task> {
    type Task = Task;
    type SpawnOption = ();

    fn spawn_opt(&self, t: impl Into<Self::Task>, _opt: &()) {
        let u = SchedUnit::new(t.into(), ());
        let sched_time = u.sched_time();
        self.injector.push(u);
        self.core.unpark_any(sched_time);
    }
}

impl<Task: Send> Queue for SimpleQueue<Task> {
    fn deque(&mut self, metrics: &mut HandleMetrics) -> (Steal<SchedUnit<Self::Task>>, bool) {
        if let Some(e) = self.local.pop() {
            metrics.handled_local += 1;
            return (Steal::Success(e), true);
        }

        let mut need_retry = false;
        match self.remote.injector.steal_batch_and_pop(&self.local) {
            e @ Steal::Success(_) => {
                metrics.handled_global += 1;
                return (e, false);
            }
            Steal::Retry => need_retry = true,
            _ => {}
        }
        for stealer in &self.stealers {
            match stealer.steal_batch_and_pop(&self.local) {
                e @ Steal::Success(_) => {
                    metrics.handled_steal += 1;
                    return (e, false);
                }
                Steal::Retry => need_retry = true,
                _ => {}
            }
        }
        if need_retry {
            metrics.handled_retry += 1;
            (Steal::Retry, false)
        } else {
            metrics.handled_miss += 1;
            (Steal::Empty, false)
        }
    }
}
