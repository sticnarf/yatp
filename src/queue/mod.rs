pub mod simple;

use crate::core::{HandleMetrics, SchedUnit};
use crate::LocalSpawn;

use crossbeam_deque::Steal;

pub trait Queue: LocalSpawn {
    fn deque(
        &mut self,
        metrics: &mut HandleMetrics,
    ) -> (Steal<SchedUnit<Self::Task, Self::TaskContext>>, bool);
}
