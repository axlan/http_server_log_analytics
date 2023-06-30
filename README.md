# http_server_log_analytics
Python scripts for generating site usage analytics from server side "Extended Log File Format" logs.

Specifically meant to process logs generated from AWS CloudFront with logging to an S3 bucket enabled: <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html>.

**Note: This is a fairly quick and dirty implementation.**

# Original Version

`python update_combined_logs.py out/` processes any newly downloaded logs and add them to a CSV with the needed values.

Then use `python run_dashboard.py` to start a dashboard showing site usage.

`run.sh` syncs the files from S3 then runs the other two scripts.

Files could be added from a different source instead of AWS as well.

# More Efficient Version

After about a year, the scaling of running this processing was starting to unmanageable. So I rewrote this scripts in a way that only captures the data I'm interested in.

`python daily_metrics_generator.py out/` processes any newly downloaded logs and add them to [Feater](https://arrow.apache.org/docs/python/feather.html) file.

Then use `python daily_metrics_dashboard.py` to start a dashboard showing site usage.

`daily_metrics_run.sh` syncs the files from S3 then runs the other two scripts.
