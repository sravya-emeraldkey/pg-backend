import os
import json
import boto3
import botocore
import botocore.session as bc
from botocore.client import Config
import time
import io
import csv

# Initialize S3 client and other clients
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
secret_name = os.environ['SecretId']  # Getting SecretId from Environment variables
session = boto3.session.Session()
region = session.region_name

# Initializing Secret Manager's client    
client = session.client(
    service_name='secretsmanager',
    region_name=region
)

get_secret_value_response = client.get_secret_value(
    SecretId=secret_name
)
secret_arn = get_secret_value_response['ARN']
secret = get_secret_value_response['SecretString']
secret_json = json.loads(secret)
cluster_id = secret_json['dbClusterIdentifier']

# Initializing Redshift's client   
config = Config(connect_timeout=5, read_timeout=5)
client_redshift = session.client("redshift-data", config=config)

def lambda_handler(event, context):
    print(f"Entered lambda_handler: {event}")
    file_key = event['file_key']
    bucket_name = event['bucket_name']
    start_index = event.get('start_index', 0)  # Default start index to 0
    lambda_timeout_buffer = 840  # 14 minutes buffer

    try:
        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        csv_file = io.StringIO(file_content)
        csv_reader = list(csv.DictReader(csv_file))  # Read all rows into a list

        start_time = time.time()

        for index, row in enumerate(csv_reader[start_index:], start=start_index):
            elapsed_time = time.time() - start_time
            if elapsed_time >= lambda_timeout_buffer:
                print(f"Timeout approaching. Reinvoking Lambda at row {index}")
                invoke_self(context, file_key, bucket_name, index)
                print(f"Reinvoked Lambda at row {index}")
                return {
                    'statusCode': 202,
                    'body': f'Reinvoked Lambda at row {index}'
                }

            try:
                # Handle optional fields and substitute NULL where necessary
                lead_id = row.get('Id') or 'NULL'
                source = row.get('Lead Source') or 'NULL'
                lead_status = row.get('Status') or 'NULL'
                lead_score = row.get('Lead Score #') or 'NULL'
                creation_date = row.get('Date Added') or 'NULL'

                # Wrap strings in single quotes if not NULL
                source = f"'{source}'" if source != 'NULL' else source
                lead_status = f"'{lead_status}'" if lead_status != 'NULL' else lead_status
                creation_date = f"'{creation_date}'" if creation_date != 'NULL' else creation_date
                lead_score = f"'{lead_score}'" if lead_score != 'NULL' else lead_score

                delete_sql_query = f"""
                DELETE FROM public.Lead WHERE Lead_ID = {lead_id};
                """
                insert_sql_query = f"""
                INSERT INTO public.Lead (Lead_ID, Source, Lead_Status, Lead_Score, Creation_Date)
                VALUES ({lead_id}, {source}, {lead_status}, {lead_score}, {creation_date});
                """
                # Execute delete query first
                execute_redshift_query(delete_sql_query)

                # Execute insert query
                execute_redshift_query(insert_sql_query)

            except Exception as row_error:
                print(f"Error processing row {row}: {str(row_error)}")

        print("CSV processing completed successfully")
        return {
            'statusCode': 200,
            'body': 'Records processed successfully!'
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Failed to process file: {str(e)}"
        }

def execute_redshift_query(query_str):
    """
    Execute a query in the Redshift cluster.
    """
    print(f"Executing query: {query_str}")
    try:
        result = client_redshift.execute_statement(
            Database='dev',
            SecretArn=secret_arn,
            Sql=query_str,
            ClusterIdentifier=cluster_id
        )
        print(f"Query executed successfully: {result}")
        return result

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise

def invoke_self(context, file_key, bucket_name, start_index):
    """
    Reinvoke the same Lambda function with updated start_index.
    """
    lambda_client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps({
            "file_key": file_key,
            "bucket_name": bucket_name,
            "start_index": start_index
        })
    )
