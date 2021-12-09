use std::path::Path;
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};

use crate::{machine::Machine, Config, Fallible, SSH_OPTS};

#[derive(Serialize, Deserialize, Clone)]
pub struct Task {
    pub name: String,
    pub cmd: String,
    pub repeat: Option<usize>,
}

impl Task {
    pub fn start(&self, config: &Config, machine: &Machine) -> Fallible {
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

    pub fn check(&self, config: &Config, binary: &Path, machine: &Machine) -> Fallible<bool> {
        println!("Checking task {} on machine {}", self.name, machine.name);

        let binary_file_name = binary
            .file_name()
            .ok_or("Missing binary file name")?
            .to_str()
            .ok_or("Invalid binary file name")?;

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

    pub fn fetch_results(&self, config: &Config, machine: &Machine) -> Fallible {
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
