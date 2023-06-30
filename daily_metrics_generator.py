from functools import reduce

from multiprocessing import Pool, Queue

from dataclasses import dataclass

from collections import defaultdict

# To get logs from s3 use aws-cli:
#   aws s3 sync s3://$BUCKET_NAME/ out/
import gzip
import sys
import io
import os
from typing import List, Optional, Dict
import urllib.parse

import pandas as pd
from ua_parser import user_agent_parser

from datetime import datetime

OUT_FILE = "daily_metrics.feather"

PREFIX = "E3SR3H7C34DQ6Z."

MAX_LINE_LEN = 1024

NUM_THREADS = 8


@dataclass
class MetricsByDateAndPage:
    date: datetime
    page: str
    human_total_requests: int = 0
    human_unique_requests: int = 0
    bot_total_requests: int = 0
    bot_unique_requests: int = 0


def process_func(files: str):
    if len(files) == 0:
        return []
    current_date = get_date(files[0])
    days_files = []
    metrics = []
    for file in files:
        new_date = get_date(file)
        if new_date == current_date:
            days_files.append(file)
            continue

        df = load_files(days_files)
        # Pandas only infers correct type for datetime.datetime (not datetime.date)
        current_datetime = datetime(
            current_date.year, current_date.month, current_date.day
        )
        metrics += extract_analytic_data(current_datetime, df).values()

        current_date = new_date
        days_files = [file]

    return metrics


def get_date(file: str) -> datetime.date:
    parts = file.split(".")
    return datetime.strptime(parts[1], "%Y-%m-%d-%H").date()


def load_files(files_to_load: List[str]) -> pd.DataFrame:
    START_STR = "#Version:"
    df = None

    for file_path in files_to_load:
        try:
            with open(file_path, "rb") as test_fd:
                peek_data = test_fd.peek(len(START_STR))
                if peek_data.decode("ascii", errors="ignore").startswith("#Version:"):
                    file_fd = open(file_path, "r")
                else:
                    data = gzip.decompress(test_fd.read())
                    file_fd = io.StringIO(data.decode("ascii"))

            # Skip version line
            file_fd.readline()
            # Read column header
            names = file_fd.readline().split(" ")[1:]
            file_df = pd.read_csv(file_fd, names=names, delimiter="\t")
            if df is None:
                df = file_df
            else:
                df = pd.concat([df, file_df])

        except Exception as e:
            print(f"Couldn't open file: {file_path}. {str(e)}")
            continue

    return df


def extract_analytic_data(
    date: datetime, df: pd.DataFrame
) -> Dict[str, MetricsByDateAndPage]:
    df = df[(df["cs-uri-stem"].str.endswith("/")) & (df["sc-status"] == 200)]

    data: Dict[str, MetricsByDateAndPage] = {}
    ips: Dict[str, set] = defaultdict(set)

    for _, row in df.iterrows():
        page = row["cs-uri-stem"]
        ip = row["c-ip"]
        if page not in data:
            data[page] = MetricsByDateAndPage(date=date, page=page)

        ua_string = urllib.parse.unquote(row["cs(User-Agent)"])
        ua_data = user_agent_parser.Parse(ua_string)
        device_data = ua_data["device"]["family"]
        os_data = ua_data["os"]["family"]
        agent_data = ua_data["user_agent"]["family"]
        is_uninque = ip not in ips[page]
        if is_uninque:
            ips[page].add(ip)

        if is_bot(device_data, os_data, agent_data):
            data[page].bot_total_requests += 1
            if is_uninque:
                data[page].bot_unique_requests += 1
        else:
            data[page].human_total_requests += 1
            if is_uninque:
                data[page].human_unique_requests += 1
    return data


def is_bot(c_device, c_os, c_agent) -> bool:
    return (
        c_device == "Other"
        or c_os == "Other"
        or c_agent == "Other"
        or c_device == "Spider"
    )


def main():
    path_arg = sys.argv[1]
    out_path = os.path.join(path_arg, OUT_FILE)

    files = []
    for f in os.listdir(path_arg):
        if f.startswith(PREFIX):
            files.append(f)
    files = sorted(files)

    print(f"{len(files)} total files")

    old_df = None
    last_date = None

    if os.path.exists(out_path):
        old_df = pd.read_feather(out_path)
        old_df.info()
        last_date = old_df["date"].max().date()

    used_file_count = 0
    start_day = None
    last_idx = 0
    file_allocations = [[] for _ in range(NUM_THREADS)]
    for f in files:
        date = get_date(f)
        if last_date and date <= last_date:
            continue

        if start_day is None:
            start_day = date
        used_file_count += 1
        idx = (date - start_day).days
        if idx - last_idx > 1:
            print(f"Mising days {last_idx}-{idx}")
        last_idx = idx
        file_allocations[idx % NUM_THREADS].append(os.path.join(path_arg, f))

    print(f"{used_file_count} files to process")

    if used_file_count == 0:
        return

    with Pool(NUM_THREADS) as p:
        metrics = p.map(process_func, file_allocations)
    metrics = reduce(lambda x, y: x + y, metrics, [])
    metrics = sorted(metrics, key=lambda v: v.date)

    df = pd.DataFrame(metrics)
    df["date"] = df["date"].astype("datetime64[D]")
    df["human_total_requests"] = df["human_total_requests"].astype("uint16")
    df["human_unique_requests"] = df["human_unique_requests"].astype("uint16")
    df["bot_total_requests"] = df["bot_total_requests"].astype("uint16")
    df["bot_unique_requests"] = df["bot_unique_requests"].astype("uint16")

    if old_df is not None:
        df = pd.concat([old_df, df])
    df["page"] = df["page"].astype("category")

    size_all = len(df)

    last_day = df["date"].max()
    df = df[df["date"] != last_day]
    df.reset_index(inplace=True)

    print(f"Dropping {size_all - len(df)} results for current day")

    df.info()

    # df.to_csv(OUT_FILE,  index=False)
    df.to_feather(out_path)


if __name__ == "__main__":
    main()
