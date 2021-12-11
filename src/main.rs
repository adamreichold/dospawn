use std::collections::VecDeque;
use std::env::args_os;
use std::error::Error;
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

mod job;
mod machine;
mod task;

use crate::{job::Job, machine::Machine};

fn main() -> Fallible {
    let path = args_os().nth(1).ok_or("Missing path argument")?;

    let mut job = Job::read(&path)?;

    while job.machines.len() < job.max_machines() {
        let machine = Machine::create(
            format!("{}-{}", job.config.name, job.machines.len()),
            &job.config,
        )?;
        job.machines.push(machine);

        job.write(&path)?;
    }

    let mut todo = VecDeque::new();

    for (machine_idx, machine) in job.machines.iter().enumerate() {
        if machine.tasks.iter().all(|task| task.is_none()) {
            machine.copy_binary_and_inputs(&job)?;
            machine.install_required_software(&job.config)?;
        }

        for task_idx in 0..job.config.tasks_per_machine {
            todo.push_back((Instant::now(), machine_idx, task_idx));
        }
    }

    while let Some((deadline, machine_idx, task_idx)) = todo.pop_front() {
        if let Some(duration) = deadline.checked_duration_since(Instant::now()) {
            sleep(duration);
        }

        let machine = &mut job.machines[machine_idx];

        if let Some(task) = &machine.tasks[task_idx] {
            let finished = task.check(&job.config, machine)?;

            if finished || job.config.fetch_partial_results {
                task.fetch_results(&job.config, machine)?;
            }

            if finished {
                machine.tasks[task_idx] = None;

                job.write(&path)?;
            }
        }

        let machine = &mut job.machines[machine_idx];

        if machine.tasks[task_idx].is_none() {
            if let Some(task) = Job::next_task(&mut job.tasks) {
                task.start(&job.config, machine)?;

                machine.tasks[task_idx] = Some(task);

                job.write(&path)?;
            } else {
                if machine.tasks.iter().all(|task| task.is_none()) {
                    machine.delete()?;

                    let mut todo_idx = 0;

                    while todo_idx < todo.len() {
                        let (_, machine_idx1, _) = &mut todo[todo_idx];

                        if *machine_idx1 == machine_idx {
                            todo.remove(todo_idx);
                        } else {
                            if *machine_idx1 == job.machines.len() - 1 {
                                *machine_idx1 = machine_idx;
                            }

                            todo_idx += 1;
                        }
                    }

                    job.machines.swap_remove(machine_idx);

                    job.write(&path)?;
                }

                continue;
            }
        }

        todo.push_back((
            Instant::now() + Duration::from_secs(job.config.check_interval),
            machine_idx,
            task_idx,
        ));
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub max_machines: usize,
    pub tasks_per_machine: usize,
    pub name: String,
    pub image: String,
    pub size: String,
    pub region: String,
    pub ssh_key: String,
    pub ssh_user: String,
    pub install_cmd: String,
    pub check_interval: u64,
    #[serde(default)]
    pub fetch_partial_results: bool,
}

const SSH_OPTS: &[&str] = &[
    "-q",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
];

type Fallible<T = ()> = Result<T, Box<dyn Error>>;
