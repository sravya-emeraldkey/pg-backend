import boto3
import json
import csv
import os
import io

# Initialize AWS clients
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # S3 Bucket and File Information
        source_bucket = os.environ.get("INFO_BUCKET")  # Replace with your source bucket name
        target_bucket = os.environ.get("INFO_BUCKET")  # Same bucket or another one for CSV
        target_file_name = "callrail_output_file.csv"  # Name of the output CSV file

        # List all JSON files in the source bucket
        json_files = list_json_files(source_bucket)
        
        # Prepare the CSV data
        csv_data = []
        # headers = ['id', 'startTime']  # ringcentral headers
        headers = ['id', 'start_time'] # callrail headers

        for json_file in json_files:
            print(f"Processing file: {json_file}")
            file_data = s3_client.get_object(Bucket=source_bucket, Key=json_file)
            json_content = json.loads(file_data['Body'].read().decode('utf-8'))
            
            # Extract only the required fields ('id' and 'startTime')
                # if(json_content.get('recording')):
            if isinstance(json_content, dict):
                record = {
                    'id': json_content.get('id'),
                    # 'startTime': json_content.get('startTime') #for ringcentral
                    'start_time': json_content.get('start_time'), #for callrail
                }
                csv_data.append(record)

        # Check if we have any data to write to the CSV
        if not csv_data:
            raise Exception("No valid data found in the JSON files to write to CSV.")

        # Write CSV data to a CSV file in memory
        output = io.StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=headers)
        csv_writer.writeheader()
        csv_writer.writerows(csv_data)

        # Upload the CSV data to the target S3 bucket
        s3_client.put_object(Bucket=target_bucket, Key=target_file_name, Body=output.getvalue())
        print(f"CSV file saved to {target_bucket}/{target_file_name}")
        
        return {
            'statusCode': 200,
            'body': 'CSV file successfully created and uploaded!'
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error processing files: {str(e)}"
        }

def list_json_files(bucket_name):
    """List all JSON files in the specified S3 bucket, handling pagination."""
    json_files = []
    continuation_token = None

    while True:
        # List objects with a continuation token if it exists
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(Bucket=bucket_name)

        # Process the list of files
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".json"):
                json_files.append(obj["Key"])

        # Check if there are more files
        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break  # No more files, exit loop

    return json_files
