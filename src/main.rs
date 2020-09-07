use std::collections::VecDeque;
use std::env::args;
use std::error::Error;
use std::fs::{read, write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_yaml::{from_slice, to_vec};

fn main() -> Fallible {
    let path = args().nth(1).ok_or_else(|| "Missing path argument")?;

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

    while !job.machines.is_empty() {
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
                if let Some(task) = next_task(&mut job.tasks) {
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

        sleep(Duration::from_secs(60));
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Job {
    binary: PathBuf,
    inputs: Vec<PathBuf>,
    config: Config,
    machines: Vec<Machine>,
    tasks: VecDeque<Task>,
}

impl Job {
    fn read<P: AsRef<Path>>(path: P) -> Fallible<Self> {
        let job = from_slice(&read(path)?)?;

        Ok(job)
    }

    fn write<P: AsRef<Path>>(&self, path: P) -> Fallible {
        write(path, &to_vec(self)?)?;

        Ok(())
    }

    fn max_machines(&self) -> usize {
        let max_tasks = self
            .tasks
            .iter()
            .map(|task| task.repeat.map_or(1, |repeat| repeat.max(1)))
            .sum::<usize>();

        max_tasks.min(self.config.max_machines)
    }
}

#[derive(Serialize, Deserialize)]
struct Config {
    max_machines: usize,
    name: String,
    image: String,
    size: String,
    region: String,
    ssh_key: String,
    ssh_user: String,
    install_cmd: String,
}

#[derive(Serialize, Deserialize)]
struct Machine {
    name: String,
    id: String,
    ip: String,
    task: Option<Task>,
}

impl Machine {
    fn create(name: String, config: &Config) -> Fallible<Self> {
        println!("Creating machine {}", name);

        let doctl = Command::new("doctl")
            .args(&[
                "compute",
                "droplet",
                "create",
                "--wait",
                "--image",
                &config.image,
                "--size",
                &config.size,
                "--region",
                &config.region,
                "--ssh-keys",
                &config.ssh_key,
                "--format",
                "ID,PublicIPv4",
                "--no-header",
            ])
            .arg(&name)
            .stderr(Stdio::inherit())
            .output()?;

        if !doctl.status.success() {
            return Err(format!("Failed to create machine {}", name).into());
        }

        let stdout = String::from_utf8(doctl.stdout)?;
        let mut fields = stdout.split_whitespace();

        let id = fields
            .next()
            .ok_or_else(|| "Missing Droplet ID")?
            .to_owned();
        let ip = fields
            .next()
            .ok_or_else(|| "Missing Droplet IP")?
            .to_owned();

        Ok(Self {
            name,
            id,
            ip,
            task: None,
        })
    }

    fn install_required_software(&self, config: &Config) -> Fallible {
        println!("Installing required software on machine {}", self.name);

        let ssh = Command::new("ssh")
            .args(SSH_OPTS)
            .arg(format!("{}@{}", config.ssh_user, self.ip))
            .arg("--")
            .arg(&config.install_cmd)
            .status()?;

        if !ssh.success() {
            return Err(format!(
                "Failed to install required bundles on machine {}",
                self.name,
            )
            .into());
        }

        Ok(())
    }

    fn copy_binary_and_inputs(&self, job: &Job) -> Fallible {
        println!("Copying binary and inputs to machine {}", self.name);

        let scp = Command::new("scp")
            .args(SSH_OPTS)
            .arg("-C")
            .arg(&job.binary)
            .args(&job.inputs)
            .arg(format!("{}@{}:", job.config.ssh_user, self.ip))
            .status()?;

        if !scp.success() {
            return Err(
                format!("Failed to copy binary and inputs to machine {}", self.name,).into(),
            );
        }

        Ok(())
    }

    fn delete(&self) -> Fallible {
        println!("Deleting machine {}", self.name);

        let doctl = Command::new("doctl")
            .args(&["compute", "droplet", "delete", "--force"])
            .arg(&self.id)
            .status()?;

        if !doctl.success() {
            return Err(format!("Failed to delete machine {}", self.name).into());
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct Task {
    name: String,
    cmd: String,
    repeat: Option<usize>,
}

impl Task {
    fn start(&self, config: &Config, machine: &Machine) -> Fallible {
        println!("Starting task {} on machine {}", self.name, machine.name);

        let cmd = format!(
            "rm -rf {name} && mkdir {name} && cd {name} && (nohup {cmd} >stdout 2>stderr &)",
            name = self.name,
            cmd = self.cmd,
        );

        let ssh = Command::new("ssh")
            .args(SSH_OPTS)
            .arg(format!("{}@{}", config.ssh_user, machine.ip))
            .arg("--")
            .arg(cmd)
            .status()?;

        if !ssh.success() {
            return Err(format!(
                "Failed to start task {} on machine {}",
                self.name, machine.name
            )
            .into());
        }

        Ok(())
    }

    fn check(&self, config: &Config, binary: &Path, machine: &Machine) -> Fallible<bool> {
        println!("Checking task {} on machine {}", self.name, machine.name);

        let binary_file_name = binary
            .file_name()
            .ok_or_else(|| "Missing binary file name")?
            .to_str()
            .ok_or_else(|| "Invalid binary file name")?;

        let cmd = format!("pidof {}", binary_file_name);

        let ssh = Command::new("ssh")
            .args(SSH_OPTS)
            .arg(format!("{}@{}", config.ssh_user, machine.ip))
            .arg("--")
            .arg(cmd)
            .stdout(Stdio::null())
            .status()?;

        Ok(!ssh.success())
    }

    fn fetch_results(&self, config: &Config, machine: &Machine) -> Fallible {
        println!(
            "Fetching results of task {} from machine {}",
            self.name, machine.name
        );

        let rsync = Command::new("rsync")
            .arg("-e")
            .arg(format!("ssh {}", SSH_OPTS.join(" ")))
            .arg("--recursive")
            .arg("--delete")
            .arg("--inplace")
            .arg("--compress")
            .arg(format!("{}@{}:{}/", config.ssh_user, machine.ip, self.name))
            .arg(&self.name)
            .status()?;

        if !rsync.success() {
            return Err(format!(
                "Failed to fetch results of task {} from machine {}",
                self.name, machine.name,
            )
            .into());
        }

        Ok(())
    }
}

fn next_task(tasks: &mut VecDeque<Task>) -> Option<Task> {
    let mut task = tasks.pop_front()?;

    if let Some(repeat) = task.repeat {
        if repeat > 1 {
            let mut task = task.clone();
            task.repeat = Some(repeat - 1);
            tasks.push_back(task);
        }

        task.name += &format!("_{}", repeat);
        task.repeat = None;
    }

    Some(task)
}

const SSH_OPTS: &[&str] = &[
    "-q",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
];

type Fallible<T = ()> = Result<T, Box<dyn Error>>;
