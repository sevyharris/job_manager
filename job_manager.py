import os
from abc import ABC, abstractmethod


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

    def completed(self):
        raise NotImplementedError

    def submit(self, command):
        raise NotImplementedError


practice_job = SlurmJob()
my_cmd = "sbatch practice_job.sh"
