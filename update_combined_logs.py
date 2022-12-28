# To get logs from s3 use aws-cli:
#   aws s3 sync s3://$BUCKET_NAME/ out/
import gzip
import sys
import io
import os
from typing import List
import urllib.parse

import pandas as pd
from ua_parser import user_agent_parser

OUT_FILE = 'combined_logs.csv'
LAST_FILE = 'last_entry.txt'
RESERVED_FILES = ['.gitignore', OUT_FILE, LAST_FILE]

def load_files(files_to_load: List[str]) -> pd.DataFrame:
    START_STR = "#Version:"
    df = None

    for file_path in files_to_load:
        try:
            with open(file_path, 'rb') as test_fd:
                peek_data = test_fd.peek(len(START_STR))
                if peek_data.decode('ascii', errors="ignore").startswith("#Version:"):
                    file_fd = open(file_path, 'r')
                else:
                    data = gzip.decompress(test_fd.read())
                    file_fd = io.StringIO(data.decode('ascii'))

            # Skip version line
            file_fd.readline()
            # Read column header
            names = file_fd.readline().split(' ')[1:]
            file_df = pd.read_csv(file_fd, names=names, delimiter='\t')
            if df is None:
                df = file_df
            else:
                df = pd.concat([df, file_df])    

        except Exception as e:
            print(f"Couldn't open file: {file_path}. {str(e)}")
            continue

    return df


def extract_analytic_data(df: pd.DataFrame) -> pd.DataFrame:
    return_df = df[['c-ip', 'cs-uri-stem', 'sc-status', 'cs(Referer)']].copy()
    return_df['times'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    device_data = []
    os_data = []
    agent_data = []
    
    for ua_string in df['cs(User-Agent)']:
        ua_string = urllib.parse.unquote(ua_string)
        ua_data = user_agent_parser.Parse(ua_string)
        device_data.append(ua_data['device']['family'])
        os_data.append(ua_data['os']['family'])
        agent_data.append(ua_data['user_agent']['family'])

    return_df['c-device'] = device_data
    return_df['c-os'] = os_data
    return_df['c-agent'] = agent_data

    return return_df.reset_index(drop=True)


def filter_bots(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df['c-device'] != 'Other') | (df['c-os'] != 'Other') | (df['c-os'] != 'Other')]
    df = df[df['c-device'] != 'Spider']
    return df
    

def main():
    path_arg = sys.argv[1]

    files_to_load = sorted(f for f in os.listdir(path_arg) if f not in RESERVED_FILES)
    num_files_in_dir = len(files_to_load)

    print(f'{num_files_in_dir} logs in directory.')

    last_file_path = os.path.join(path_arg, LAST_FILE)
    out_file_path = os.path.join(path_arg, OUT_FILE)
    append_results = False
    if os.path.exists(last_file_path) and os.path.exists(out_file_path):
        append_results = True
        with open(last_file_path, 'r') as fd:
            last_file = fd.read()
        print(f'Previous combined log found, resuming from {last_file}.')
        try:
            idx = files_to_load.index(last_file) + 1
            if idx != len(files_to_load):
                files_to_load = files_to_load[idx:]
                print(f'{len(files_to_load)} new logs.')
            else:
                print('No new logs.')
                return
        except:
            pass

    last_file = files_to_load[-1]
    paths_to_load = [os.path.join(path_arg, f) for f in files_to_load]

    df = load_files(paths_to_load)
    df = extract_analytic_data(df)

    mode = 'a' if append_results else 'w'
    with open(out_file_path, mode, newline='\n', encoding='utf-8') as fd:
        df.to_csv(fd, index=False)

    with open(last_file_path, 'w') as fd:
        fd.write(last_file)

if __name__ == '__main__':
    main()
