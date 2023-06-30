#!/usr/bin/env bash
BUCKET_NAME=robopenguins-cloudfront-logs

aws s3 sync s3://$BUCKET_NAME/ out/

python daily_metrics_generator.py out/
python daily_metrics_dashboard.py
