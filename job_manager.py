from abc import ABC, abstractmethod
from rmgpy.rmg import output
import subprocess
import os


class Job(ABC):
    """A generic job class for submitting compute jobs.
    """
    def __init__(self):
        pass

    @abstractmethod
    def completed(self):
        raise NotImplementedError

    @abstractmethod
    def submit(self, command):
        raise NotImplementedError


class SlurmJob(Job):
    """A job class for submitting compute jobs to SLURM on discovery
    """
    def __init__(self):
        self.job_id = 0
        self.status = "NEW"

    def completed(self):
        if self.job_id == 0:
            raise ValueError("Job has not been submitted!")
        call_sacct = f'sacct -j {self.job_id} --format=JobID,JobName,State'
        cmd_pieces = call_sacct.split()
        process = subprocess.Popen(cmd_pieces, stdout=subprocess.PIPE)
        output = process.stdout.read().decode('utf-8')
        # confirm the header order
        lines = output.split('\n')
        headers = lines[0].split()
        if headers[0] != "JobID" or headers[1] != "JobName" or headers[2] != "State":
            raise ValueError("sacct output does not match expected format")
        if len(lines) == 3:
            raise ValueError(f'No job matching ID {self.job_id}')

        # Check the first line for the answer
        if str(self.job_id) != lines[2].split()[0]:
            raise ValueError(f'first line does not match job id {self.job_id}')      
        self.status = lines[2].split()[2]
        if self.status == "COMPLETED":
            return True
        return False
        # TODO test all statuses: failed, pending, running, completing
        # All job codes listed here: https://slurm.schedmd.com/sacct.html

    def failed(self):
        if self.status == "FAILED":
            return True
        return False

    def running(self):
        if self.status == "RUNNING":
            return True
        return False

    def pending(self):
        if self.status == "PENDING":
            return True
        return False

    def submit(self, command):
        cmd_pieces = command.split()
        process = subprocess.Popen(cmd_pieces, stdout=subprocess.PIPE)
        output = process.stdout.read().decode('utf-8')
        self.job_id = int(output.split()[-1])
        print(output)


class SlurmJobFile():
    """A class for creating SLURM scripts
    """
    def __init__(self, full_path=''):
        self.path = full_path
        self.settings = {
            '--job-name': None,
            '--error': 'error.log',
            '--output': 'output.log',
            '--nodes': 1,
            '--partition': 'west,short',
            '--exclude': 'c5003',
            '--mem': '8Gb',
            '--time': '1:00:00',
            '--cpus-per-task': 4,
            '--array': None
        }
        self.content = []  # lines of commands to include

    def write_file(self):
        fdir, fname = os.path.split(self.path)
        if not os.path.exists(fdir):
            raise OSError(f"File path does not exist {fdir}")
        with open(self.path, "w") as writer:
            writer = open(self.path, "w")
            writer.write('#!/bin/bash\n')
            for setting_name in self.settings.keys():
                if self.settings[setting_name] is not None:
                    writer.write(f'#SBATCH {setting_name}={self.settings[setting_name]}\n')
            writer.write('\n\n')
            writer.writelines(self.content)
