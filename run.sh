#!/usr/bin/env bash
BUCKET_NAME=robopenguins-cloudfront-logs

aws s3 sync s3://$BUCKET_NAME/ out/

python update_combined_logs.py out/
python run_dashboard.py