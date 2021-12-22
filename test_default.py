import time
import job_manager


practice_job = job_manager.DefaultJob()
my_cmd = "/bin/bash simple_script.sh"

print("submitting job")
practice_job.submit(my_cmd)
print("job submitted")


for i in range(0, 10):
    complete = practice_job.completed()
    print(i, complete)
    if complete:
        break
    time.sleep(1)


#time.sleep(3)
#if practice_job.completed():
#    print("practice job has completed")
#else:
#    print(f'practice job is {practice_job.status}')


