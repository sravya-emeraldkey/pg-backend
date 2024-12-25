import os
import requests
import json
import boto3
import uuid

# CallRail API details
API_KEY = os.environ.get('API_KEY')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID')
BASE_URL = f'https://api.callrail.com/v3/a/{ACCOUNT_ID}/calls.json?fields=agent_email,call_type,lead_status,note,source,total_calls,speaker_percent,keywords,campaign,milestones,timeline_url,person_id,transcription,keywords_spotted,call_highlights,zip_code'

# Initialize S3 client
s3_client = boto3.client('s3')
INFO_BUCKET = os.environ.get('INFO_BUCKET')     # Bucket for call data JSON
RECORDINGS_BUCKET = os.environ.get('RECORDINGS_BUCKET')  # Bucket for call recordings

def lambda_handler(event, context):
    # API request headers
    headers = {
        'Authorization': f'Token token={API_KEY}',
        'Content-Type': 'application/json'
    }

    # Pagination parameters
    per_page = 100  # Limit per page
    page = 1     # Start with the first page

    try:
        while True:
            # Fetch calls with pagination
            params = {'per_page': per_page, 'page': page, 'date_range':'yesterday'}
            response = requests.get(BASE_URL, headers=headers, params=params)
            # Check if the request was successful
            if response.status_code == 200:
                call_data = response.json()  # Parse JSON response
                calls = call_data.get('calls', [])
                print(f"Number of calls: {len(calls)}")
                # return
                # Process and save each call
                for call in calls:
                    # Create a unique file name for the JSON data
                    call_id = call.get('id', str(uuid.uuid4()))  # Use Call ID or a UUID
                    file_key = f"{call_id}.json"
                    
                    # Convert call data to JSON string
                    call_json = json.dumps(call)
                    
                    # Upload JSON file to S3
                    s3_client.put_object(
                        Bucket=INFO_BUCKET,
                        Key=file_key,
                        Body=call_json,
                        ContentType='application/json'
                    )
                    print(f"Call data saved to S3 bucket: {file_key}")
                    
                    # Download and save the recording
                    recording_url = call.get('recording')
                    if recording_url:
                        download_and_save_recording(recording_url, call_id)
                    # calls = []

                # Check if we need to fetch more pages
                if len(calls) < per_page:
                    # Break if fewer calls than `per_page` are returned (end of data)
                    print("No more calls to fetch. Exiting pagination loop.")
                    break

                # Increment page number for the next request
                page += 1
                print(f"page number: {page}")
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
                return {
                    'statusCode': response.status_code,
                    'body': f"Error fetching call data: {response.text}"
                }

        return {
            'statusCode': 200,
            'body': f"All available calls fetched and saved successfully to S3!"
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

def download_and_save_recording(recording_url, call_id):
    try:
        # Step 1: Fetch the JSON containing the redirect URL
        headers = {'Authorization': f'Token token={API_KEY}'}
        response = requests.get(recording_url, headers=headers)
        response.raise_for_status()  # Ensure the request was successful

        # Parse the JSON response to get the actual download URL
        recording_data = response.json()
        final_url = recording_data.get('url')
        if not final_url:
            print(f"No valid download URL for Call ID {call_id}")
            return

        # Step 2: Download the MP3 from the final URL
        final_response = requests.get(final_url, allow_redirects=True)
        final_response.raise_for_status()  # Ensure successful download

        # Step 3: Save the recording to S3
        file_key = f"{call_id}.mp3"
        s3_client.put_object(
            Bucket=RECORDINGS_BUCKET,
            Key=file_key,
            Body=final_response.content,
            ContentType='audio/mpeg'
        )
        print(f"Recording saved to S3: {file_key}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading recording for Call ID {call_id}: {e}")
    except Exception as e:
        print(f"Unexpected error for Call ID {call_id}: {e}")

