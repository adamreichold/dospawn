use std::env::args;
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

use serde::{Deserialize, Serialize};

mod job;
mod machine;
mod task;

use crate::{job::Job, machine::Machine};

fn main() -> Fallible {
    let path = args().nth(1).ok_or("Missing path argument")?;

    let mut job = Job::read(&path)?;

    while job.machines.len() < job.max_machines() {
        let machine = Machine::create(
            format!("{}-{}", job.config.name, job.machines.len()),
            &job.config,
        )?;
        job.machines.push(machine);

        job.write(&path)?;
    }

    for machine in &job.machines {
        if machine.task.is_none() {
            machine.install_required_software(&job.config)?;
            machine.copy_binary_and_inputs(&job)?;
        }
    }

    loop {
        let mut idx = 0;

        while idx < job.machines.len() {
            let machine = &mut job.machines[idx];

            if let Some(task) = &machine.task {
                let finished = task.check(&job.config, &job.binary, machine)?;

                task.fetch_results(&job.config, machine)?;

                if finished {
                    machine.task = None;

                    job.write(&path)?;
                }
            }

            let machine = &mut job.machines[idx];

            if machine.task.is_none() {
                if let Some(task) = Job::next_task(&mut job.tasks) {
                    task.start(&job.config, machine)?;

                    machine.task = Some(task);

                    idx += 1;
                } else {
                    machine.delete()?;

                    job.machines.remove(idx);
                }

                job.write(&path)?;
            } else {
                idx += 1;
            }
        }

        if job.machines.is_empty() {
            break;
        } else {
            sleep(Duration::from_secs(300));
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub max_machines: usize,
    pub name: String,
    pub image: String,
    pub size: String,
    pub region: String,
    pub ssh_key: String,
    pub ssh_user: String,
    pub install_cmd: String,
}

const SSH_OPTS: &[&str] = &[
    "-q",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
];

type Fallible<T = ()> = Result<T, Box<dyn Error>>;
