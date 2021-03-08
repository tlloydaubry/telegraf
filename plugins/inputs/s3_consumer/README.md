# S3 Consumer Input Plugin

This plugin provides a consumer for use with AWS S3.

New files written to an S3 bucket are read and parsed for metrics. An S3 Bucket is configured to send S3 events (optionally for a specific prefix and suffix) to an SQS queue from which telegraf will process the events and read any new files created and read any metrics in data_format.
S3Events are read from an SQS queue subscribed to an SNS topic subscribed to an S3 Bucket and path. For each s3:ObjectCreated event read the file and metrics into memory.


