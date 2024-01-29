from abc import ABC, abstractmethod
import subprocess
import os
import time
import re


def get_user():
    cmd = "whoami"
    cmd_pieces = cmd.split()
    proc = subprocess.Popen(cmd_pieces, stdin=None, stdout=subprocess.PIPE, stderr=None, close_fds=True)
    for line in iter(proc.stdout.readline, ''):
        text = line.strip().decode('utf-8')
        if text != '':
            return text

def count_slurm_jobs(): 
    user = get_user()
    cmd = f"squeue -u {user}"
    cmd_pieces = cmd.split()
    proc = subprocess.Popen(cmd_pieces, stdin=None, stdout=subprocess.PIPE, stderr=None, close_fds=True)
    count = 0
    i = 0
    for line in iter(proc.stdout.readline, ''):
        text = line.strip().decode('utf-8')
        if text != '' and user[:8] in text:
            # print(text)
            count += 1
        i += 1
        if i > 200:
            break
    return count


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


class DefaultJob(Job):
    """A job class for your local machine if you don't have SLURM
    """
    def __init__(self):
        self.job_id = 0
        self.status = "NEW"
        proc = None

    def get_status(self):
        return self.status

    def completed(self):
        if self.proc is None:
            return False
        poll = self.proc.poll()
        if poll is None:
            # p.subprocess is alive
            return False
        return True

    def submit(self, command):
        # submit the process. Don't wait for completion.
        cmd_pieces = command.split()
        # process = subprocess.Popen(cmd_pieces, stdout=subprocess.PIPE)
        # process = subprocess.run(cmd_pieces, stdin=None, stdout=None, stderr=None, close_fds=True)
        self.proc = subprocess.Popen(cmd_pieces, stdin=None, stdout=None, stderr=None, close_fds=True)
        print(self.proc)

    def wait(self, check_interval=60):
        """waits for the job to complete, checking every check_interval
        seconds to see if it completed. Does not yet work for arrays
        """
        while not self.completed():
            time.sleep(check_interval)


class SlurmJob(Job):
    """A job class for submitting compute jobs to SLURM on discovery
    """
    def __init__(self):
        self.job_id = 0
        self.status = "NEW"
        self._last_sacct_lines = []
        self._array_jobs = []

    def _get_sacct(self, job_id=None):
        if job_id is None:
            job_id = self.job_id
        if job_id == 0:
            raise ValueError("Job has not been submitted!")
        call_sacct = f'sacct -j {job_id} --format=JobID,JobName,State'
        cmd_pieces = call_sacct.split()
        process = subprocess.Popen(cmd_pieces, stdout=subprocess.PIPE)
        output = process.stdout.read().decode('utf-8')
        # confirm the header order
        lines = output.split('\n')
        headers = lines[0].split()
        if headers[0] != "JobID" or headers[1] != "JobName" or headers[2] != "State":
            raise ValueError("sacct output does not match expected format")
        if len(lines) == 3:
            # pretty sure this means the job isn't on the board yet...
            # wait 2 more seconds and try again
            time.sleep(2.0)
            self._get_sacct()
            if not self._last_sacct_lines:
                print(f'cmd was {cmd_pieces}')
                raise ValueError(f'No job matching ID {job_id}')
        self._last_sacct_lines = lines

    def _check_array_pending(self):
        self._get_sacct()
        lines = self._last_sacct_lines
        new_status = self.get_status()
        if new_status == 'PENDING':
            return True
        return False

    def _get_jobs_in_array(self):
        self._get_sacct()
        lines = self._last_sacct_lines
        job_ids = []
        for i, line in enumerate(lines):
            if i < 2 or len(line) == 0:
                continue
            matches = re.search('[0-9]*_[0-9]{1,3}$', line.split()[0])
            if matches is not None:
                job_ids.append(matches.group(0))
        job_ids = list(set(job_ids))
        job_ids.sort()
        if len(job_ids) == 0:
            raise ValueError(f'Job {self.job_id} is not an array job')
        self._array_jobs = job_ids

    def get_status(self, job_id=None):
        if job_id is None:
            job_id = self.job_id
        self._get_sacct(job_id)
        if str(job_id) != self._last_sacct_lines[2].split()[0] and str(job_id) not in self._last_sacct_lines[2].split()[0]:
            raise ValueError(f'first line does not match job id {self.job_id}')
        status = self._last_sacct_lines[2].split()[2]
        return status

    def completed(self):
        self._get_sacct()

        # Check the first line for the answer
        if str(self.job_id) != self._last_sacct_lines[2].split()[0] and str(self.job_id) not in self._last_sacct_lines[2].split()[0]:
            raise ValueError(f'first line does not match job id {self.job_id}')
        self.status = self._last_sacct_lines[2].split()[2]
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
        try:
            self.job_id = int(output.split()[-1])
        except IndexError:
            # this usually means you have an error in your SLURM script
            print(f'cmd was {command}')
            print(output)
            raise
        print(output)

    def wait(self, check_interval=60):
        """waits for the job to complete, checking every check_interval
        seconds to see if it completed. Does not yet work for arrays
        """

        job_done = False
        while not job_done:
            time.sleep(check_interval)

            # TODO fix the logic here- it's not ideal that we rely on
            # completed() to actually check the status and all the others
            # just check the object variable that function returned
            # should create a helper function like check_status() that
            # all the others call
            if self.completed():
                job_done = True
            elif self.status in ("FAILED", "CANCELLED", "DEADLINE", "OUT_OF_MEMORY", "PREEMPTED", "TIMEOUT"):
                job_done = True

    def wait_all(self, check_interval=60):
        """waits for all of the jobs in the array to complete,
        checking every check_interval seconds to see if it completed
        """
        # need to make sure it's at least running, and this is a terrible hack
        time.sleep(10)
        # part of the problem might be that the jobs can take longer than 10 seconds to get running. But they should at least be pending by then

        # wait for the job to not be pending
        while self._check_array_pending():
            time.sleep(10)

        self._get_jobs_in_array()
        for array_id in self._array_jobs:
            while True:
                status = self.get_status(job_id=array_id)
                if status in ("COMPLETED", "FAILED", "CANCELLED", "DEADLINE", "OUT_OF_MEMORY", "PREEMPTED", "TIMEOUT",
                              "COMPLETED+", "FAILED+", "CANCELLED+", "DEADLINE+", "OUT_OF_MEMORY+", "PREEMPTED+", "TIMEOUT+"):
                    break
                else:
                    time.sleep(check_interval)


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


class CobaltJobFile():
    """A class for creating COBALT scripts
    """
    def __init__(self, full_path=''):
        self.path = full_path
        self.settings = {
            '-n': 1,
            '-t': '01:10:20',  # 1 hour, 10 minutes 20 seconds
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
                    writer.write(f'#COBALT {setting_name}={self.settings[setting_name]}\n')
            writer.write('\n\n')
            writer.writelines(self.content)

