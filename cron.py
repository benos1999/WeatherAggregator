# cron.py
import schedule
import time
import subprocess

def job():
       subprocess.run(["python", "weather_api_export.py"])


schedule.every().hour.at(":01").do(job)
while True:
       schedule.run_pending()
       time.sleep(60)