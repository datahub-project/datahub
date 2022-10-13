import os
import datetime
import  subprocess
import logging
import shutil
import glob
import ntpath

logging.basicConfig(filename="/dump/debug_log.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.info("Multielasticdump process started")

logger = logging.getLogger()

es_host = os.environ.get("ELASTIC_HOST",None)
retention_days = int(os.environ.get("RETENTION_PERIOD",14)) #since env is read as str
if not es_host:    
    es_host='http://elasticsearch:9200'    
logger.info(f"es_host is {es_host}")


curr_date = datetime.datetime.now()
curr_folder_name = curr_date.strftime("%Y-%m-%d_%H-%M")
curr_compressed_file = f"/dump/{curr_folder_name}.tar.gz"
curr_folder_path = f"/dump/{curr_folder_name}"
if not os.path.exists(curr_folder_path):    
    try:
        os.makedirs(curr_folder_path)
        logger.info(f"folder {curr_folder_path} created!")
    except:
        logger.error(f"folder {curr_folder_path} already exist.")    
subprocess.run(["multielasticdump", f"--input={es_host}", "--direction=dump", f"--output={curr_folder_path}"])
logger.info(f"MED dump for {curr_folder_name} completed!")
subprocess.run(["tar","cvzf", f"{curr_compressed_file}",f"{curr_folder_path}"])
logger.info(f"{curr_compressed_file} compressed file generated")
shutil.rmtree(curr_folder_path)
logger.info(f"{curr_folder_path} removed!")

all_backups=glob.glob("/dump/*.tar.gz")
for backup_file in all_backups:
    present_date = ntpath.basename(backup_file).replace(".tar.gz", "")
    parsed_date = datetime.datetime.strptime(present_date, "%Y-%m-%d_%H-%M")
    if int((curr_date-parsed_date).days) > retention_days:
        os.remove(backup_file)
        logger.info(f"housekeep: removing {backup_file}")
logger.info(f"End of MED.")
