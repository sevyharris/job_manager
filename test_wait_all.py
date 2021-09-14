import time
import job_manager


practice_job = job_manager.SlurmJob()
practice_job.job_id = 21072107

practice_job.wait_all()

