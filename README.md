# serverless
The Lambda function will be invoked by the SNS notification.
The Lambda function is responsible for following:
Download the release from the GitHub repository and store it in Google Cloud Storage Bucket.
Email the user the status of download.
Track the emails sent in DynamoDB.