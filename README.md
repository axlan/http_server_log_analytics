# http_server_log_analytics
Python scripts for generating site usage analytics from server side "Extended Log File Format" logs.

Specifically meant to process logs generated from AWS CloudFront with logging to an S3 bucket enabled: <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html>.

**Note: This is a fairly quick and dirty implementation.**

`python update_combined_logs.py out/` processes any newly downloaded logs and add them to a CSV with the needed values.

Then use `python run_dashboard.py` to start a dashboard showing site usage.

`run.sh` syncs the files from S3 then runs the other two scripts.

Files could be added from a different source instead of AWS as well.
