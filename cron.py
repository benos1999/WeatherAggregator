# cron.py
import schedule
import time
import subprocess

def job():
       subprocess.run(["python", "weather_api_export.py"])

schedule.every().hour.do(job)

while True:
       schedule.run_pending()
       time.sleep(60)