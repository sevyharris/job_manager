import time
import job_manager


practice_job = job_manager.SlurmJob()
my_cmd = "sbatch /home/harris.se/slurm_practice/practice_job.sh"

practice_job.submit(my_cmd)
time.sleep(3)
if practice_job.completed():
    print("practice job has completed")
else:
    print(f'practice job is {practice_job.status}')


# Test the SlurmJobFile writer
testfile = job_manager.SlurmJobFile(full_path="/home/moon/methanol/meOH-synthesis/perturbed_runs/autotask.sh")
testfile.content = ['# The first line of content\n', '# second line of content\n']
testfile.write_file()
