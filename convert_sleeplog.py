import pandas as pd
import os
from os import listdir
from os.path import isfile, join

def convert_sleep_log(filepath: str):
    df = pd.read_csv(filepath)
    
    # Convert 'time stamp' to datetime.time format
    df['time stamp'] = pd.to_datetime(df['time stamp'], format='%Y-%m-%d %H:%M:%S:%f', errors='coerce').dt.time

    df['marker'] = [f"onset_N{(i//2)+1}" if i % 2 == 0 # even rows are onset time stamps 
              else f"wakeup_N{(i//2)+1}"         # odd rows are wakeup time stamps
              for i in range(len(df))] 
        
    sleep_log = df[['marker', 'time stamp']].T
    
    # Set first row (marker) as header 
    new_header = sleep_log.iloc[0] 
    sleep_log = sleep_log[1:] 
    sleep_log.columns = new_header 
    
    # Add ID column as first column
    filename = os.path.basename(filepath)
    sleep_log.insert(loc = 0, column = 'ID', value = os.path.splitext(filename)[0])
    
    return(sleep_log)

def bulk_convert_sleep_logs(in_dir: str, out_path: str = "sleep_log.csv"):
    if not os.path.exists(in_dir):
        raise FileNotFoundError(f"Input directory '{in_dir}' does not exist.")
    
    logs = pd.DataFrame()

    for entry in os.scandir(in_dir):
        # Check if entry is a valid csv file
        if entry.is_file() and entry.name.endswith('.csv'):
            sleep_log = convert_sleep_log(join(in_dir, entry.name))
            logs = pd.concat([logs, sleep_log], axis=0)
    
    logs.to_csv(out_path, index = False) 
