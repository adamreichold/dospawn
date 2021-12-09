use std::process::{Command, Stdio};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{job::Job, task::Task, Config, Fallible, SSH_OPTS};

#[derive(Serialize, Deserialize)]
pub struct Machine {
    pub name: String,
    pub id: String,
    pub ip: String,
    pub task: Option<Task>,
    pub next_check: SystemTime,
}

impl Machine {
    pub fn create(name: String, config: &Config) -> Fallible<Self> {
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

        let id = fields.next().ok_or("Missing Droplet ID")?.to_owned();
        let ip = fields.next().ok_or("Missing Droplet IP")?.to_owned();

        Ok(Self {
            name,
            id,
            ip,
            task: None,
            next_check: SystemTime::now() + config.check_interval,
        })
    }

    pub fn copy_binary_and_inputs(&self, job: &Job) -> Fallible {
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

    pub fn install_required_software(&self, config: &Config) -> Fallible {
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

    pub fn delete(&self) -> Fallible {
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

    pub fn next_check(next_check: &mut SystemTime, config: &Config) -> bool {
        let now = SystemTime::now();

        if *next_check <= now {
            *next_check = now + config.check_interval;

            true
        } else {
            false
        }
    }
}
