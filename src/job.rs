use std::collections::VecDeque;
use std::fs::{read, write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_yaml::{from_slice, to_vec};

use crate::{machine::Machine, task::Task, Config, Fallible};

#[derive(Serialize, Deserialize)]
pub struct Job {
    pub binary: PathBuf,
    pub inputs: Vec<PathBuf>,
    pub config: Config,
    pub machines: Vec<Machine>,
    pub tasks: VecDeque<Task>,
}

impl Job {
    pub fn read<P: AsRef<Path>>(path: P) -> Fallible<Self> {
        let job = from_slice(&read(path)?)?;

        Ok(job)
    }

    pub fn write<P: AsRef<Path>>(&self, path: P) -> Fallible {
        write(path, &to_vec(self)?)?;

        Ok(())
    }

    pub fn max_machines(&self) -> usize {
        let max_tasks = self
            .tasks
            .iter()
            .map(|task| task.repeat.map_or(1, |repeat| repeat.max(1)))
            .sum::<usize>();

        max_tasks.min(self.config.max_machines)
    }

    pub fn next_check(&self) -> Option<Duration> {
        let next_check = self
            .machines
            .iter()
            .map(|machine| machine.next_check)
            .min()
            .unwrap();

        next_check.duration_since(SystemTime::now()).ok()
    }

    pub fn next_task(tasks: &mut VecDeque<Task>) -> Option<Task> {
        let mut task = tasks.pop_front()?;

        if let Some(repeat) = task.repeat {
            if repeat > 1 {
                let mut task = task.clone();
                task.repeat = Some(repeat - 1);
                tasks.push_back(task);
            }

            let repeat = repeat.to_string();
            task.name = task.name.replace("{{repeat}}", &repeat);
            task.cmd = task.cmd.replace("{{repeat}}", &repeat);
            task.repeat = None;
        }

        Some(task)
    }
}
