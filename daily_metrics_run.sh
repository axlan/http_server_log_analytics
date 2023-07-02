#!/usr/bin/env bash
BUCKET_NAME=robopenguins-cloudfront-logs
PREFIX="E3SR3H7C34DQ6Z."

python daily_metrics_generator.py --s3-logs=$BUCKET_NAME --prefix=$PREFIX
python daily_metrics_dashboard.py
