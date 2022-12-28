# http_server_log_analytics
Python scripts for generating site usage analytics from server side "Extended Log File Format" logs.

Specifically meant to process logs generated from AWS CloudFront with logging to an S3 bucket enabled: <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html>.

Use `aws s3 sync s3://$BUCKET_NAME/ out/` to download the log files.

`python update_combined_logs.py out/` to process any newly downloaded logs and add them to a CSV with the needed values.

Then use `python run_dashboard.py` to start a dashboard showing site usage.

Files could be added from a different source instead of AWS as well.
