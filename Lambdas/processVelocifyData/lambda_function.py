import boto3
import csv
import io
import requests
import time
import json
import os

# Initialize S3 client and Lambda client
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    
    # S3 bucket and file information
    target_bucket_name = os.environ.get('target_bucket_name')
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    #Invoke redshift lambda
    invoke_redshift_lambda(file_key,source_bucket_name)
    # source_bucket_name = os.environ.get('source_bucket_name')
    # file_key = os.environ.get('file_key')

    # Start tracking time
    start_time = time.time()
    lambda_timeout_buffer = 840  # 14 minutes buffer

    # Get the start index from the event or default to 0
    start_index = event.get("start_index", 0)
    print(f"startIndex: {start_index}")
    try:
        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=source_bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')

        # Use io.StringIO to treat the string content as a file
        csv_file = io.StringIO(file_content)
        csv_reader = list(csv.DictReader(csv_file))  # Read all rows into a list
        
        for index, row in enumerate(csv_reader[start_index:], start=start_index):
            # Check remaining time
            elapsed_time = time.time() - start_time
            if elapsed_time >= lambda_timeout_buffer:
                print(f"Timeout approaching. Reinvoking Lambda at row {index}")
                invoke_self(context, file_key, index)
                print(f"Reinvoked Lambda at row {index}")
                return {
                    'statusCode': 202,
                    'body': f'Reinvoked Lambda at row {index}'
                }

            # Process the row
            recording_url = row.get("Recording")
            if recording_url and recording_url.startswith("http"):
                # Download the recording file
                recording_response = requests.get(recording_url)
                if recording_response.status_code == 200:
                    call_id = row.get("Call Id", "unknown")
                    target_file_key = f"{call_id}.mp3"
                    # Save the recording to the target S3 bucket
                    s3_client.put_object(
                        Bucket=target_bucket_name,
                        Key=target_file_key,
                        Body=recording_response.content
                    )
                else:
                    print(f"Failed to download recording: {recording_url}, Status Code: {recording_response.status_code}")
        print("Recordings processed and saved successfully")
        return {
            'statusCode': 200,
            'body': 'Recordings processed and saved successfully!'
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Failed to process file: {str(e)}"
        }

def invoke_self(context, file_key, start_index):
    """Reinvoke the same Lambda function."""
    lambda_client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',  # Async invocation
        Payload=json.dumps({
            "file_key": file_key,
            "start_index": start_index
        })
    )


def invoke_redshift_lambda(file_key,bucket_name):
    """Invoke the redshift Lambda function."""
    lambda_client.invoke(
        FunctionName="pushVelocifyCallLogsToRedshift",
        InvocationType='Event',  # Async invocation
        Payload=json.dumps({
            "file_key": file_key,
            "bucket_name": bucket_name
        })
    )
