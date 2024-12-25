import os
import boto3
import json
import requests
from requests.auth import HTTPBasicAuth
import uuid
import base64
import time

# RingCentral API Credentials
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
BASE_URL = os.environ.get('BASE_URL')
SECRET_NAME = os.environ.get('SECRET_NAME')
# S3 Buckets
INFO_BUCKET = os.environ.get('INFO_BUCKET')
RECORDINGS_BUCKET = os.environ.get('RECORDINGS_BUCKET')

# Initialize S3 client
s3_client = boto3.client("s3")
# Initialize Secret Manager
secrets_manager = boto3.client('secretsmanager')

def lambda_handler(event, context):
    try:
        # Step 1: Authenticate with RingCentral
        jwt_token = get_jwt_token_from_secrets()
        access_token = get_access_token(jwt_token)
        # print(f"access_token: {access_token}")
        # return
        # Start with the initial page
        next_page_uri = None  
        per_page = 100
        
        # Step 2: Fetch call logs and handle pagination
        while True:
            call_logs = fetch_call_logs(access_token, next_page_uri, per_page)
            
            # Process the current page's records
            records = call_logs.get("records", [])
            print(f"Number of records in this page: {len(records)}")
            
            for call in records:
                # Save call info to S3
                call_id = call.get("id", str(uuid.uuid4()))
                save_to_s3(INFO_BUCKET, f"{call_id}.json", call)
                
                # Uncomment below if you need to download recordings
                recording_url = call.get("recording", {}).get("contentUri")
                if recording_url:
                    download_and_save_recording(recording_url, call_id, access_token)
            
            # Move to the next page if available
            next_page_uri = call_logs.get("navigation", {}).get("nextPage", {}).get("uri")
            print(f"nextPage: {next_page_uri}")
            if not next_page_uri:
                print("No more pages to fetch.")
                break  # Exit loop if no next page available
        
        return {
            "statusCode": 200,
            "body": "Call logs and recordings processed successfully!"
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
      
def get_jwt_token_from_secrets():
    try:
        # Fetch the secret from Secrets Manager
        response = secrets_manager.get_secret_value(SecretId=SECRET_NAME)
        
        # Check if the secret value is in the 'SecretString' field
        secret = response['SecretString']
        secret_dict = json.loads(secret)
        
        # Extract the JWT token from the secret
        jwt_token = secret_dict.get('RingCentral/JWTToken')
        if jwt_token:
            return jwt_token
        else:
            raise Exception("JWT token not found in Secrets Manager.")
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        raise
    
def get_access_token(jwt_token):
    url = "https://platform.ringcentral.com/restapi/oauth/token"
    
    # Prepare Basic Auth value using base64 encoding of client_id and client_secret
    client_credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(client_credentials.encode("utf-8")).decode("utf-8")
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}"
    }
    
    # Data for the request body
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",  # The grant type for JWT Bearer flow
        "assertion": jwt_token  # The JWT token generated in the previous step
    }

    # Make the POST request to get the access token
    response = requests.post(url, data=data, headers=headers)
    
    if response.status_code == 200:
        # Return the access token from the response
        return response.json()["access_token"]
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None
        
        
def fetch_call_logs(access_token, next_page_uri=None, per_page=100):
    """Fetch call logs from RingCentral."""
    if next_page_uri:
        # Use the provided next_page_uri for pagination
        url = next_page_uri
        params = None  # Params are already included in the next_page_uri
    else:
        # Initial call (first page)
        url = f"{BASE_URL}/restapi/v1.0/account/~/call-log"
        params = {
            "perPage": per_page,
            "type": "Voice",  # Fetch voice calls only
            "withRecording": 'TRUE',
            "dateFrom": "1970-01-01T00:00:00Z"
        }

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch call logs: {response.status_code}, {response.text}")
        raise Exception("Failed to fetch call logs")




def save_to_s3(bucket_name, file_name, content, content_type="application/json"):
    """Save content to S3 bucket."""
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(content) if content_type == "application/json" else content,
        ContentType=content_type
    )
    print(f"Saved to S3: {bucket_name}/{file_name}")

def download_and_save_recording(recording_url, call_id, access_token):
    """Download the call recording and save it to S3."""
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(recording_url, headers=headers, allow_redirects=True)
    if response.status_code == 200:
        file_name = f"{call_id}.mp3"
        save_to_s3(RECORDINGS_BUCKET, file_name, response.content, content_type="audio/mpeg")
        print(f"Recording saved: {file_name}")
    else:
        print(f"Failed to download recording for Call ID {call_id}. URL: {recording_url}")
    time.sleep(0.5)
    
def download_and_save_recording_new(recording_url, call_id, access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    retries = 0
    backoff = 1  # Start with 1 second backoff

    while retries < 5:  # Allow up to 5 retries
        try:
            response = requests.get(recording_url, headers=headers, allow_redirects=True)
            if response.status_code == 200:
                file_name = f"{call_id}.mp3"
                save_to_s3(RECORDINGS_BUCKET, file_name, response.content, content_type="audio/mpeg")
                print(f"Recording saved: {file_name}")
                return
            elif response.status_code == 429:  # Rate limit exceeded
                retries += 1
                print(f"Rate limit exceeded, retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2  # Double the delay on each retry
            else:
                print(f"Failed to download recording for Call ID {call_id}. Status: {response.status_code}, Error: {response.text}")
                break
        except requests.RequestException as e:
            print(f"Request error for Call ID {call_id}: {str(e)}")
            break
    print(f"Failed to download recording after {retries} retries.")