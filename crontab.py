import datetime
import luigi
from apscheduler.schedulers.blocking import BlockingScheduler
import os
from process_file import CalculateAveragePoint

def scheduled_job():
    dt = datetime.datetime.now()
    print(f"{dt} | worker started")
    
    folder_to_watch = '/home/lap14088/Documents/luigi_average_point/submits'
    for new_file in os.listdir(folder_to_watch):
        file_path = os.path.join(folder_to_watch, new_file)
        if os.path.isfile(file_path):
            print(f"File: {new_file}. Task to average points")
            task = CalculateAveragePoint(input_file=file_path)
            luigi.build([task], local_scheduler=True)

if __name__ == '__main__':
    sched = BlockingScheduler()
    sched.add_job(scheduled_job, 'interval', id='my_job_id', seconds=5)
    print("Scheduler started...")
    sched.start()
