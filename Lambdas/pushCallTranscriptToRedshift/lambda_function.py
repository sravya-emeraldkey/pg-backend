import os
import json
import boto3
import botocore
import io
import csv
import time
from datetime import datetime
from botocore.client import Config
import uuid


# Initialize S3 client and Secrets Manager client
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
secret_name = os.environ['SecretId']
session = boto3.session.Session()
region = session.region_name

# Secrets Manager client
client_secretsmanager = session.client(service_name='secretsmanager', region_name=region)
get_secret_value_response = client_secretsmanager.get_secret_value(SecretId=secret_name)
secret_arn = get_secret_value_response['ARN']
secret_json = json.loads(get_secret_value_response['SecretString'])
cluster_id = secret_json['dbClusterIdentifier']

# Redshift client
config = Config(connect_timeout=5, read_timeout=5)
client_redshift = session.client("redshift-data", config=config)

def lambda_handler(event, context):
    print(f"Entered lambda_handler: {event}")
    transcript = {
        "call_id":"93224940-18BE-407B-BC26-D807E8FB4D90",
        "transcript":"Test"
    }
    transcript_id = uuid.uuid4()
    call_id = transcript['call_id']
    transcript =  transcript['transcript']
    transcript_id = f"'{transcript_id}'" if transcript_id != 'NULL' else transcript_id
    call_id = f"'{call_id}'" if call_id != 'NULL' else call_id
    transcript = f"'{transcript}'" if transcript != 'NULL' else transcript
    try:
        # Construct SQL queries to delete and insert
        delete_sql_query = f"""
        DELETE FROM public.Transcript WHERE Call_ID = {call_id};
        """
        insert_sql_query = f"""
        INSERT INTO public.Transcript (Transcript_ID,Call_ID,Transcript)
        VALUES ({transcript_id},{call_id},{transcript});
        """

        # Execute delete query first
        execute_redshift_query(delete_sql_query)

        # Execute insert query
        execute_redshift_query(insert_sql_query)

        print("CSV processing completed successfully")
        return {
            'statusCode': 200,
            'body': 'Call logs processed successfully!'
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