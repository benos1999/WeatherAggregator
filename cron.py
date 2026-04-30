import schedule
import time
import subprocess
import logging
import sys
from datetime import datetime

# 1. Setup logging to output directly to Railway's dashboard
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def job():
    logging.info("--- Starting Hourly Weather Export ---")
    try:
        # 2. Run the script and capture errors
        # timeout=300 ensures the job kills itself if it hangs for more than 5 minutes
        result = subprocess.run(
            ["python", "weather_api_export.py"],
            capture_output=True,
            text=True,
            timeout=300 
        )
        
        # Log the output of your weather script
        if result.stdout:
            logging.info(f"Script Output: {result.stdout.strip()}")
            
        if result.returncode == 0:
            logging.info("Success: weather_api_export.py finished perfectly.")
        else:
            logging.error(f"Script Error (Code {result.returncode}): {result.stderr}")

    except subprocess.TimeoutExpired:
        logging.error("Failure: The weather script took too long and was killed.")
    except Exception as e:
        logging.error(f"Unexpected error during execution: {str(e)}")
    
    logging.info("--- Job Cycle Finished ---")

# 3. Schedule for every hour
schedule.every().hour.at(":01").do(job)

logging.info("Cron scheduler started. Monitoring for :01 trigger...")

while True:
    schedule.run_pending()
    # 4. Sleep for a shorter interval (1 second) 
    # This ensures we never "jump over" the :01 mark.
    time.sleep(1)