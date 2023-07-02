import gzip
import io
from typing import List

import pandas as pd
import boto3


def s3_url_to_parts(s3_url: str):
    scheme = "s3://"
    if not s3_url.lower().startswith(scheme):
        return None, None
    s3_url = s3_url[len(scheme) :]

    parts = s3_url.split("/")
    if len(parts) == 1:
        return parts[0], None
    else:
        return parts[0], parts[1]


def load_extended_log_s3(bucket, log_prefix) -> pd.DataFrame:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    df = None
    for obj in bucket.objects.filter(Prefix=log_prefix):
        try:
            data = gzip.decompress(obj.get()["Body"].read())
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
            print(f"Couldn't open file: {obj.key}. {str(e)}")
            continue
    return df


def load_extended_log_files(files_to_load: List[str]) -> pd.DataFrame:
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
