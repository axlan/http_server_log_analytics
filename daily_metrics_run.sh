#!/usr/bin/env bash
BUCKET_NAME=robopenguins-cloudfront-logs
CACHE_LOCATION=s3://jdiamond-personal-backups/daily_metrics.feather
PREFIX="E3SR3H7C34DQ6Z."

python daily_metrics_generator.py --s3-logs=$BUCKET_NAME --prefix=$PREFIX --cache=$CACHE_LOCATION
python daily_metrics_dashboard.py
