import os
import json
import boto3
import time

# Initialize S3 client and Lambda client
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Environment variables
bucket_name = os.environ.get('bucket_name')  # Your S3 bucket name
lambda_timeout_buffer = 840  # Buffer for Lambda timeout (14 minutes)

def lambda_handler(event, context):
    print(f"Entered lambda_handler: {event}")
    
    # Get the current time
    current_time = time.time()
    
    # Get the start index from the event or default to 0
    start_index = event.get("start_index", 0)
    print(f"Start Index: {start_index}")
    
    # List all objects in the bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    objects = response.get('Contents', [])
    
    # Start tracking time
    start_time = time.time()

    for index, obj in enumerate(objects[start_index:], start=start_index):
        elapsed_time = time.time() - start_time
        if elapsed_time >= lambda_timeout_buffer:
            print(f"Timeout approaching. Reinvoking Lambda at object {index}")
            invoke_self(context, index)
            return {
                'statusCode': 202,
                'body': f"Reinvoked Lambda at object {index}"
            }

        # Get the last modified time of the object
        last_modified = obj['LastModified'].timestamp()

        # Check if the object is older than 30 days (2592000 seconds)
        if current_time - last_modified > 2592000:
            print(f"Deleting object: {obj['Key']}")
            # Delete the object
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

    return {
        'statusCode': 200,
        'body': 'Old call logs deleted successfully!'
    }

def invoke_self(context, start_index):
    """Reinvoke the same Lambda function."""
    lambda_client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',  # Async invocation
        Payload=json.dumps({
            "start_index": start_index
        })
    )
