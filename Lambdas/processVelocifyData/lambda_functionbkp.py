import boto3
import csv
import io
import requests

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # S3 bucket and file information
    source_bucket_name = "velocify-calls"  # Source bucket with the CSV file
    target_bucket_name = "velocify-recordings"  # Target bucket for recordings
    file_key = "Lm32481_CallHistory_20241119_205746_6b80c371-a30a-4933-a83e-b57a5d38c303-part2.csv";

    try:
        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=source_bucket_name, Key=file_key)
        
        # Read content as bytes and decode to string
        file_content = response['Body'].read().decode('utf-8')

        # Use io.StringIO to treat the string content as a file
        csv_file = io.StringIO(file_content)
        
        # Read CSV content
        csv_reader = csv.DictReader(csv_file)
        
        for row in csv_reader:
            recording_url = row.get("Recording")  # Get the 'Recording' column
            if recording_url and recording_url.startswith("http"):
                print(f"Downloading recording: {recording_url}")
                
                # Download the recording file
                recording_response = requests.get(recording_url)
                
                if recording_response.status_code == 200:
                    # Generate a unique file name for the recording
                    call_id = row.get("Call Id", "unknown")
                    target_file_key = f"{call_id}.mp3"

                    # Save the recording to the target S3 bucket
                    s3_client.put_object(
                        Bucket=target_bucket_name,
                        Key=target_file_key,
                        Body=recording_response.content
                    )
                    print(f"Recording saved to S3: {target_bucket_name}/{target_file_key}")
                else:
                    print(f"Failed to download recording: {recording_url}, Status Code: {recording_response.status_code}")
            else:
                print("No valid recording URL found for row:", row)

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
