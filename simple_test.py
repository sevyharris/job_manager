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
