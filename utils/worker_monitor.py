#!/usr/bin/env python3

import argparse
import email.message
import json
import os
import socket
import subprocess
from pathlib import Path
from smtplib import SMTP

from psutil import Process, process_iter, wait_procs
from psutil import ZombieProcess, NoSuchProcess, AccessDenied

BASE_DIR = Path(__file__).resolve().parents[1]
EMAIL_CONFIG_PATH = Path(BASE_DIR, "config", "email_worker_restart.json")
ENV = os.environ["CS_ENV"]
HOSTNAME = socket.gethostname()
SMTP_HOST = "localhost"
SMTP_PORT = 25


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cmdline", required=True,
                        help="The command to be executed.")
    parser.add_argument("-e", "--exclude-forks", action="store_true",
                        default=False,
                        help="Exclude forked child processes in the count.")
    parser.add_argument("-m", "--min-count", type=int, required=True,
                        help="The minimum count of processes required.")
    parser.add_argument("-n", "--notify", action="store_true", default=False,
                        help="Send email notifications as needed.")
    parser.add_argument("-s", "--screen", required=True,
                        help="The name of the target screen.")

    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument("-d", "--daemon", action="store_true", default=False,
                       help="Run the monitor continuously.")
    mutex.add_argument("-D", "--dry-run", action="store_true", default=False,
                       help="Just output commands rather than running them.")

    return parser.parse_args()


class WorkerMonitor:
    """Monitor and respawn worker processes when necessary."""

    def __init__(self, cmdline, min_count, screen, exclude_forks=False,
                 notify=False, daemon=False, dry_run=False):
        self._current_process = Process()
        self.cmdline = cmdline
        self.daemon = daemon
        self.dry_run = dry_run
        self.exclude_forks = exclude_forks
        self.min_count = min_count
        self.notify = notify
        self.screen = screen

    def __repr__(self):
        process_name = cmdline.split(" ")[0] if self.cmdline else ""
        return (f"<WorkerMonitor process_name={process_name} "
                f"min_count={self.min_count}>")

    def _notify(self, count):
        """Send a notification email that workers needed to be restarted."""
        from_, recipients = self._read_email_config()
        if recipients:
            subject = f"{count} worker(s) were just restarted on {HOSTNAME}."
            body = f"{subject}\n\nWorker process: {self.cmdline}"
            for recipient in recipients:
                with SMTP(SMTP_HOST, SMTP_PORT) as smtp:
                    message = email.message.EmailMessage()
                    message["From"] = from_
                    message["To"] = recipient
                    message["Subject"] = subject
                    message.set_content(body)
                    smtp.send_message(message)

    def _read_email_config(self):
        """Read the email configuration file."""
        with open(str(EMAIL_CONFIG_PATH)) as f:
            email_config = json.loads(f.read())
            return email_config["fromaddr"], email_config["toaddrs"]

    def _screen_init(self):
        """Initialize the screen session and run the first process."""
        num_running = len(list(self.fetch_processes()))
        num_needed = max(self.min_count - num_running, 0)
        if num_needed == self.min_count:
            cmd = f"CS_ENV={ENV} screen -d -m -S {self.screen} {self.cmdline}"
            if self.dry_run:
                cmd = f"echo {cmd}"
            subprocess.Popen(cmd, shell=True).wait()

    def _screen_add_processes(self, count):
        """Add new processes to an existing screen session."""
        cmd = f"screen -S {self.screen} -X screen {self.cmdline}"
        if self.dry_run:
            cmd = f"echo {cmd}"
        for _ in range(count):
            subprocess.Popen(cmd, shell=True).wait()

    def _screen_update_env(self):
        """Ensure CS_ENV is set on all new screen windows by default."""
        cmd = f"screen -S {self.screen} -X setenv CS_ENV {ENV}"
        if self.dry_run:
            cmd = f"echo {cmd}"
        subprocess.Popen(cmd, shell=True).wait()

    def _validate_process(self, process):
        """Ensure a process meets the criteria to be included."""
        if process == self._current_process:
            return False
        if " ".join(process.cmdline()) != self.cmdline:
            return False
        if self.exclude_forks and process.parent().cmdline() == self.cmdline:
            return False
        return True

    def fetch_processes(self):
        """Fetch the running target processes."""
        for process in process_iter():
            try:
                if self._validate_process(process):
                    yield process
            except (ZombieProcess, NoSuchProcess, AccessDenied):
                pass

    def run(self):
        """Run the worker monitor."""
        if self.daemon:
            self.run_daemon()
        else:
            self.run_once()

    def run_daemon(self):
        """Run the worker monitor continuously."""
        self._screen_init()
        self._screen_update_env()
        while True:
            # Retrieves processes twice to ensure the list contains the newly
            # added processes since there is no way to return their individual
            # pids due to launching them wrapped in a screen
            processes = list(self.fetch_processes())
            num_needed = max(self.min_count - len(processes), 0)
            self._screen_add_processes(num_needed)
            if self.notify and num_needed > 0:
                self._notify(num_needed)
            processes = list(self.fetch_processes())
            wait_procs(processes, timeout=3)

    def run_once(self):
        """Perform a single run of the worker monitor."""
        self._screen_init()
        self._screen_update_env()
        num_running = len(list(self.fetch_processes()))
        num_needed = max(self.min_count - num_running, 0)
        self._screen_add_processes(num_needed)
        if self.notify and num_needed > 0:
            self._notify(num_needed)


if __name__ == "__main__":
    try:
        args = parse_args()
        manager = WorkerMonitor(**vars(args))
        manager.run()
    except KeyboardInterrupt:
        pass

