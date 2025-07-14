import os
import time
from datetime import datetime
from demo import *

PATH_LOGBOOK = os.getenv("PATH_LOGBOOK")
logbook_mt = extract(PATH_LOGBOOK, dtype={'SO NUM':str})
logbook_mt = transform_logbook(logbook_mt)

def process_etl(file):

    print(f"Start process file {file}")

    
    if file == "DATA SMR.xls":

        DIR_SMR_JKT = os.getenv("DIR_SMR_JKT")
        smr_jkt = extract(DIR_SMR_JKT, skip_rows=4)
        smr_jkt = transform_accurate(smr_jkt, logbook_mt)
        load_to_web(smr_jkt)

    elif file == "DATA SW.xls":
        DIR_SW = os.getenv("DIR_SW")
        sw_panel = extract(DIR_SW, skip_rows=4)
        sw_panel = transform_accurate(sw_panel)
        load_to_web(sw_panel)

    elif file == "SMR DIY.xls":

        DIR_SMR_DIY = os.getenv("DIR_SMR_DIY")
        smr_diy = extract(DIR_SMR_DIY, skip_rows=4)
        smr_diy = transform_accurate(smr_diy)
        load_to_web(smr_diy)

def monitoring_files(filepath:list):

    last_update = {}

    for path in filepath:
        # ini buat dapetin waktu last update filenya. nama file dan waktu terakhir update disimpan dalam bentuk dictionary
        file_name = os.path.basename(path)
        last_update[file_name] = os.path.getmtime(path)

    while True:

        for curr_path in filepath:
            # ini buat dapetin waktu update terkini atua saat filenya di overwrite
            current_update = os.path.getmtime(curr_path)
            file_name_on_update = os.path.basename(curr_path)

            if current_update != last_update[file_name_on_update]:
                time_modified = datetime.fromtimestamp(current_update)
                time_modified = time_modified.strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"File {file_name_on_update} has been modified on {time_modified}")

                process_etl(file_name_on_update)

                # waktu yang update terkini disimpan ke last update sesuai masing masing nama file
                last_update[file_name_on_update] = current_update

        
        time.sleep(5)
            

if __name__ == "__main__":

    DIR_SMR_JKT = os.getenv("DIR_SMR_JKT")
    DIR_SW = os.getenv("DIR_SW")
    DIR_SMR_DIY = os.getenv("DIR_SMR_DIY")
    

    filepath_to_monitor = [DIR_SMR_JKT, DIR_SW, DIR_SMR_DIY]


    try:
        monitoring_files(filepath_to_monitor)
    
    except KeyboardInterrupt:
        print("Monitoring Stopped.")

