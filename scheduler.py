import schedule
import logging
import time
from pipeline import extract,transform,validate,load,get_config

config=get_config()
table_name=config["table_name"]

logger=logging.getLogger("scheduler")
logger.setLevel(logging.INFO)

file_handler=logging.FileHandler("scheduler.log")
formatter=logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(file_handler)

def run_job():
    try:
        logger.info("Job Started.")
        data=extract()
        data=transform(data)
        data=validate(data)
        load(data,table_name)
        logger.info("Job Completed.")
    except Exception as e:
        logger.error(f"Job Failed: {e}")

#schedule.every().day.at("02:00").do(run_job)
schedule.every(1).minute.do(run_job)

logger.info("Scheduler Started.")

while True:
    schedule.run_pending()
    time.sleep(60)