import boto3
import json
import requests
import time
import base64

# Initialize S3 client
s3_client = boto3.client("s3")
secrets_manager = boto3.client('secretsmanager')

CLIENT_ID = "0JoTZ21EVPQdHfyMJgCdSk"
CLIENT_SECRET = "696vikeyDxFea2CrUGHken7Y0gDx0M7icdZFstWX7IxA"
BASE_URL = "https://platform.ringcentral.com"  # Use production URL for live data
# RingCentral API Credentials and Bucket Names
SECRET_NAME = "RingCentral/JWTToken"
RECORDINGS_BUCKET = "ringcentral-call-recordings"
SOURCE_BUCKET = "ringcentral-info"

def lambda_handler(event, context):
    try:
        # Step 1: Authenticate with RingCentral
        jwt_token = get_jwt_token_from_secrets()
        access_token = get_access_token(jwt_token)

        # List all JSON files in the source bucket
        response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
        if "Contents" not in response:
            print("No files found in the bucket.")
            return {
                "statusCode": 200,
                "body": "No files to process."
            }
        
        files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".json")]
        print(f"Found {len(files)} files to process.")

        for file_key in files:
            # Process each JSON file
            process_file(file_key, access_token)
        
        return {
            "statusCode": 200,
            "body": "Recordings processed successfully!"
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }

def get_jwt_token_from_secrets():
    """Retrieve the JWT token from AWS Secrets Manager."""
    try:
        response = secrets_manager.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response['SecretString'])
        jwt_token = secret.get('RingCentral/JWTToken')
        if jwt_token:
            return jwt_token
        else:
            raise Exception("JWT token not found in Secrets Manager.")
    except Exception as e:
        print(f"Error retrieving JWT token from Secrets Manager: {str(e)}")
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

def process_file(file_key, access_token):
    """Process a single JSON file from the S3 bucket."""
    try:
        # Read the JSON file from S3
        response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
        file_content = response["Body"].read().decode("utf-8")
        call_log = json.loads(file_content)  # Single call log, not a list

        # Extract details
        call_id = call_log.get("id")
        recording_url = call_log.get("recording", {}).get("contentUri")
        
        if recording_url:
            print(f"Downloading recording for Call ID: {call_id}")
            download_and_save_recording(recording_url, call_id, access_token)
        else:
            print(f"No recording found for Call ID: {call_id}")
    except Exception as e:
        print(f"Error processing file {file_key}: {str(e)}")


def download_and_save_recording(recording_url, call_id, access_token):
    time.sleep(6)
    """Download the call recording and save it to S3."""
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(recording_url, headers=headers, allow_redirects=True)
        if response.status_code == 200:
            file_name = f"{call_id}.mp3"
            save_to_s3(RECORDINGS_BUCKET, file_name, response.content, content_type="audio/mpeg")
            print(f"Recording saved: {file_name}")
        else:
            print(f"Failed to download recording for Call ID {call_id}. Status: {response.status_code}, Error: {response.text}")
    except requests.RequestException as e:
        print(f"Request error for Call ID {call_id}: {str(e)}")

def save_to_s3(bucket_name, file_name, content, content_type="application/octet-stream"):
    """Save content to S3 bucket."""
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=content,
        ContentType=content_type
    )
    print(f"Saved to S3: {bucket_name}/{file_name}")
