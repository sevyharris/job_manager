from abc import ABC, abstractmethod
import subprocess


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
