use std::env::args;
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
            machine.copy_binary_and_inputs(&job)?;
            machine.install_required_software(&job.config)?;
        }
    }

    loop {
        let mut idx = 0;

        while idx < job.machines.len() {
            let machine = &mut job.machines[idx];

            if let Some(task) = &machine.task {
                if Machine::next_check(&mut machine.next_check, &job.config) {
                    let finished = task.check(&job.config, &job.binary, machine)?;

                    if finished || job.config.fetch_partial_results {
                        task.fetch_results(&job.config, machine)?;
                    }

                    if finished {
                        machine.task = None;

                        job.write(&path)?;
                    }
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
        } else if let Some(duration) = job.next_check() {
            sleep(duration);
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
    #[serde(serialize_with = "write_duration", deserialize_with = "read_duration")]
    pub check_interval: Duration,
    #[serde(default)]
    pub fetch_partial_results: bool,
}

fn write_duration<S: Serializer>(dur: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    dur.as_secs().serialize(serializer)
}

fn read_duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let secs = u64::deserialize(deserializer)?;

    Ok(Duration::from_secs(secs))
}

const SSH_OPTS: &[&str] = &[
    "-q",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
];

type Fallible<T = ()> = Result<T, Box<dyn Error>>;
