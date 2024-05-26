import argparse
import os
import io
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from multiprocessing import Pool, Queue
from typing import Dict, List, Optional

import boto3
import pandas as pd
from ua_parser import user_agent_parser

from extended_log import load_extended_log_files, load_extended_log_s3, s3_url_to_parts

OUT_FILE = "daily_metrics.feather"

MAX_LINE_LEN = 1024

NUM_THREADS = 8

FALLBACK_START_DATE = datetime(year=2023, month=1, day=1)


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

        df = load_extended_log_files(days_files)
        # Pandas only infers correct type for datetime.datetime (not datetime.date)
        current_datetime = datetime(
            current_date.year, current_date.month, current_date.day
        )
        metrics += extract_analytic_data(current_datetime, df).values()

        current_date = new_date
        days_files = [file]

    return metrics


def process_s3_func(args, start_date: datetime.date):
    date = start_date
    metrics = []
    while date < datetime.now().date():
        prefix = args.prefix + date.strftime("%Y-%m-%d")
        print(f'Processing {date.strftime("%Y-%m-%d")}')
        df = load_extended_log_s3(args.s3_logs, prefix)
        if df is not None:
            # Pandas only infers correct type for datetime.datetime (not datetime.date)
            current_datetime = datetime(date.year, date.month, date.day)
            metrics += extract_analytic_data(current_datetime, df).values()
        date += timedelta(days=NUM_THREADS)

    return metrics


def get_date(file: str) -> datetime.date:
    parts = file.split(".")
    return datetime.strptime(parts[1], "%Y-%m-%d-%H").date()


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
    parser = argparse.ArgumentParser()
    ex_group = parser.add_mutually_exclusive_group()
    ex_group.add_argument(
        "--local-logs", help="Local directory containing webserver extended logs."
    )
    ex_group.add_argument(
        "--s3-logs", help="S3 bucket containing webserver extended logs."
    )
    parser.add_argument(
        "--prefix",
        help="S3 or local file prefix for the logs up to the year in the filename.",
    )
    parser.add_argument(
        "--cache",
        default="out/daily_metrics.feather",
        help="S3 or local path to cache (daily_metrics.feather) file. S3 paths must start with s3://",
    )
    parser.add_argument(
        "--out-dir", default="out/", help="Directory to write output to."
    )

    args = parser.parse_args()
    if args.local_logs:
        local_generator(args)
    else:
        s3_generator(args)


def get_cache_df(cache_location):
    cache_df = None
    cache_bucket, cache_key = s3_url_to_parts(cache_location)
    if cache_key:
        print("Loaded cache from S3")
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=cache_bucket, Key=cache_key)
        stream = io.BytesIO(response["Body"].read())
        cache_df = pd.read_feather(stream)
    elif os.path.exists(cache_location):
        print("Loaded cache from File")
        cache_df = pd.read_feather(cache_location)

    return cache_df

def s3_generator(args):
    last_date = FALLBACK_START_DATE

    old_df = get_cache_df(args.cache)

    if old_df is not None:
        old_df.info()
        last_date = old_df["date"].max().date()

    start_date = last_date + timedelta(days=1)
    print(f'Loading logs from {start_date} to {datetime.now().date()}')

    params = [(args, start_date + timedelta(days=i)) for i in range(NUM_THREADS)]

    with Pool(NUM_THREADS) as p:
        metrics = p.starmap(process_s3_func, params)

    save_metrics(old_df, metrics, args)


def save_metrics(old_df, metrics, args):
    out_path = os.path.join(args.out_dir, OUT_FILE) 
    metrics = reduce(lambda x, y: x + y, metrics, [])
    if len(metrics) == 0:
        print('No logs found.')
        return
    metrics = sorted(metrics, key=lambda v: v.date)

    df = pd.DataFrame(metrics)
    df["date"] = pd.to_datetime(df["date"])
    df["human_total_requests"] = df["human_total_requests"].astype("uint16")
    df["human_unique_requests"] = df["human_unique_requests"].astype("uint16")
    df["bot_total_requests"] = df["bot_total_requests"].astype("uint16")
    df["bot_unique_requests"] = df["bot_unique_requests"].astype("uint16")

    print(f"{len(df)} new metrics")

    if old_df is not None:
        df = pd.concat([old_df, df])
    df["page"] = df["page"].astype("category")

    size_all = len(df)

    last_day = df["date"].max()
    df = df[df["date"] != last_day]
    df.reset_index(drop=True, inplace=True)

    print(f"Dropping {size_all - len(df)} results for current day")

    df.info()

    # df.to_csv(OUT_FILE,  index=False)
    df.to_feather(out_path)

    cache_bucket, cache_key = s3_url_to_parts(args.cache)
    if cache_key:
        print("Uploading cache file to S3")
        s3 = boto3.resource('s3')
        s3.Bucket(cache_bucket).upload_file(out_path, cache_key)


def local_generator(args):
    path_arg = args.local_logs

    files = []
    for f in os.listdir(path_arg):
        if f.startswith(args.prefix):
            files.append(f)
    files = sorted(files)

    print(f"{len(files)} total files")

    last_date = None

    old_df = get_cache_df(args.cache)

    if old_df is not None:
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

    save_metrics(old_df, metrics, args)


if __name__ == "__main__":
    main()
