import boto3
import json
import requests
import time
import os
import base64
import uuid
from datetime import datetime, timedelta

# RingCentral API Credentials
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
BASE_URL = os.environ.get('BASE_URL')
SECRET_NAME = os.environ.get('SECRET_NAME')

# S3 Buckets
INFO_BUCKET = os.environ.get('INFO_BUCKET')
RECORDINGS_BUCKET = os.environ.get('RECORDINGS_BUCKET')

# Initialize AWS clients
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
secrets_manager = boto3.client('secretsmanager')

def lambda_handler(event, context):
    # now = datetime.utcnow()
    # date_from = (now - timedelta(days=1)).isoformat() + "Z"
    # date_to = now.isoformat() + "Z"
    # print(f"DateFrom:{date_from}")
    # print(f"DateTo:{date_to}")
    # return
    time.sleep(6)
    try:
        # Step 1: Authenticate with RingCentral
        jwt_token = get_jwt_token_from_secrets()
        access_token = get_access_token(jwt_token)

        # Step 2: Get next_page_uri from event or start fresh
        next_page_uri = event.get("next_page_uri")
        per_page = 100

        # Step 3: Start processing
        start_time = time.time()
        lambda_timeout_buffer = 500  # Reinvoke after ~8 minutes

        while True:
            call_logs = fetch_call_logs(access_token, next_page_uri, per_page)

            # Process the current page's records
            records = call_logs.get("records", [])
            print(f"Number of records in this page: {len(records)}")

            for call in records:
                # Save call info to S3
                call_id = call.get("id", str(uuid.uuid4()))
                save_to_s3(INFO_BUCKET, f"{call_id}.json", call)

                # Download recordings with rate limiting
                recording_url = call.get("recording", {}).get("contentUri")
                if recording_url:
                    download_and_save_recording(recording_url, call_id, access_token)
                else:
                    print("No recording url")
                    
            # Move to the next page if available
            next_page_uri = call_logs.get("navigation", {}).get("nextPage", {}).get("uri")
            print(f"nextPage: {next_page_uri}")
            if not next_page_uri:
                print("No more pages to fetch.")
                break  # Exit loop if no next page is available

            # Check remaining time
            elapsed_time = time.time() - start_time
            if elapsed_time >= lambda_timeout_buffer:
                print(f"Timeout approaching. Reinvoking Lambda at next_page_uri: {next_page_uri}")
                reinvoke_lambda(context, next_page_uri)
                return {
                    "statusCode": 202,
                    "body": f"Reinvoked Lambda at next_page_uri: {next_page_uri}"
                }

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

# Functions for authentication
def get_jwt_token_from_secrets():
    try:
        response = secrets_manager.get_secret_value(SecretId=SECRET_NAME)
        secret = response['SecretString']
        secret_dict = json.loads(secret)
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
    client_credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(client_credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}"
    }

    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt_token
    }

    response = requests.post(url, data=data, headers=headers)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"Error: {response.status_code} - {response.text}")
        raise Exception("Failed to get access token")
        
# Functions for fetching and processing call logs
def fetch_call_logs(access_token, next_page_uri=None, per_page=100):
    now = datetime.utcnow()
    date_from = (now - timedelta(days=1)).isoformat() + "Z"
    date_to = now.isoformat() + "Z"
    print(f"DateFrom:{date_from}")
    print(f"DateTo:{date_to}")
    if next_page_uri:
        url = next_page_uri
        params = None
    else:
        url = f"{BASE_URL}/restapi/v1.0/account/~/call-log"
        params = {
            "perPage": per_page,
            "type": "Voice",
            "withRecording": 'TRUE',
            "dateFrom": "1970-01-01T00:00:00Z",
            # "dateFrom": date_from,
            # "dateTo": date_to,
        }

    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch call logs: {response.status_code}, {response.text}")
        raise Exception("Failed to fetch call logs")

def save_to_s3(bucket_name, file_name, content, content_type="application/json"):
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(content) if content_type == "application/json" else content,
        ContentType=content_type
    )
    print(f"Saved to S3: {bucket_name}/{file_name}")

def download_and_save_recording(recording_url, call_id, access_token):
    time.sleep(6)  # Respect rate limits
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(recording_url, headers=headers, allow_redirects=True)
    if response.status_code == 200:
        file_name = f"{call_id}.mp3"
        save_to_s3(RECORDINGS_BUCKET, file_name, response.content, content_type="audio/mpeg")
        print(f"Recording saved: {file_name}")
    else:
        print(f"Failed to download recording for Call ID {call_id}. URL: {recording_url}")

# Function for reinvoking Lambda
def reinvoke_lambda(context, next_page_uri):
    lambda_client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',  # Async invocation
        Payload=json.dumps({"next_page_uri": next_page_uri})
    )
    print(f"Lambda reinvoked with next_page_uri: {next_page_uri}")
