import json
import requests
import boto3
import uuid
import os
from datetime import datetime
from google.cloud import storage
import base64

def lambda_handler(event, context):
    print(event)
    sns_msg = json.loads(event["Records"][0]["Sns"]["Message"])
    # Download the file
    response = requests.get(sns_msg["submission_url"])
    bucket_name = os.environ.get('bucket_name')

    # Save the file to /tmp directory in Lambda
    file_path = '/tmp/submission_file.zip'  # Ensure the file name and path are suitable
    submission_status = "SUCCESSFUL"

    gcs_path = ""
    # Check if the request was successful
    if response.status_code == 200:
        # Write the downloaded content to the file
        with open(file_path, 'wb') as file:
            file.write(response.content)

        # Retrieve service account key from environment variable
        decoded_gcp_key = base64.b64decode(os.environ.get('gcp_key')).decode('utf-8')

        # Create a GCP storage client using the credentials
        client = storage.Client.from_service_account_info(json.loads(decoded_gcp_key))

        bucket = client.get_bucket(bucket_name)

        gcs_path = bucket_name + sns_msg['username'] + '/' + sns_msg['assignment_id'] + '/' + str(
            sns_msg['attempt_count']) + '/' + 'submission_file.zip'
        # Upload the zip file to GCS
        blob_name = gcs_path
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
    else:
        submission_status = "FAILED"


    # Create ses client
    ses = boto3.client('ses', region_name=os.environ.get('region'))
    # Create DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('region'))
    table = dynamodb.Table(os.environ.get('ddb_table_name'))

    email_subject = "Assignnment Submitted Sucessfully"
    email_body = 'Assignment Submitted Sucessfully\nFile Path in GCS: ' + bucket_name + "/submission_file.zip"

    if submission_status == "FAILED":
        email_subject = "Assignnment Submission Failed"
        email_body = 'Assignment Submission Failed. Unable to Download URL'

    # Parameters for the email
    email_params = {
        'Destination': {
            'ToAddresses': [sns_msg["email"]],
        },
        'Message': {
            'Body': {
                'Text': {
                    'Data': email_body,
                },
            },
            'Subject': {
                'Data': email_subject,
            },
        },
        'Source': os.environ.get('source_email'),
    }

    print(email_params)

    try:
        # Send the email
        response = ses.send_email(**email_params)
        print("Email sent:", response)

        # Record sent email details in DynamoDB
        timestamp = str(datetime.now())
        email_info = {
            'unique_id': response['MessageId'],
            'emailid': sns_msg["email"],
            'submission': submission_status,
            'email_status': 'EMAIL_SENT',
            'timestamp': timestamp,
        }
        table.put_item(Item=email_info)

    except Exception as e:
        print("Error sending email:", str(e))

        # Record sent email details in DynamoDB
        timestamp = str(datetime.now())
        email_info = {
            'unique_id': str(uuid.uuid4()),
            'emailid': sns_msg["email"],
            'submission': submission_status,
            'email_status': 'EMAIL_SENDING_FAILED',
            'timestamp': timestamp,
        }
        table.put_item(Item=email_info)
