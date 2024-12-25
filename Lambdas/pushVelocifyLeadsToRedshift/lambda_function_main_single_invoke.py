import os
import json
import boto3
import botocore 
import botocore.session as bc
from botocore.client import Config
import time
import io
import csv

s3_client = boto3.client('s3')
secret_name=os.environ['SecretId'] # getting SecretId from Environment varibales
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
secret_arn=get_secret_value_response['ARN']

secret = get_secret_value_response['SecretString']

secret_json = json.loads(secret)

cluster_id=secret_json['dbClusterIdentifier']

# Initializing Botocore client
bc_session = bc.get_session()

session = boto3.Session(
        botocore_session=bc_session,
        region_name=region
    )

# Initializing Redshift's client   
config = Config(connect_timeout=5, read_timeout=5)
client_redshift = session.client("redshift-data", config = config)

def lambda_handler(event, context):
    print(f"Entered lambda_handler: {event}")
    file_key = event['file_key']
    bucket_name = event['bucket_name']
    
    try:
        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        csv_file = io.StringIO(file_content)
        csv_reader = csv.DictReader(csv_file)
        
        for row in csv_reader:
            try:
                # Handle optional fields and substitute NULL where necessary
                lead_id = row.get('Id') or 'NULL'
                source = row.get('Lead Source') or 'NULL'
                lead_status = row.get('Status') or 'NULL'
                lead_score = row.get('Lead Score #') or 'NULL'
                creation_date = row.get('Date Added') or 'NULL'
                
                # If the fields are strings, wrap them in single quotes, otherwise use raw values
                source = f"'{source}'" if source != 'NULL' else source
                lead_status = f"'{lead_status}'" if lead_status != 'NULL' else lead_status
                creation_date = f"'{creation_date}'" if creation_date != 'NULL' else creation_date
                lead_score = f"'{lead_score}'" if lead_score != 'NULL' else lead_score

                delete_sql_query = f"""
                DELETE FROM public.Lead WHERE Lead_ID = {lead_id};
                """
                # Construct the SQL query
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
        
        return {
            'statusCode': 200,
            'body': 'Recordings processed successfully!'
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Failed to process file: {str(e)}"
        }

def execute_redshift_query(query_str):
    """
    Insert a record into the Redshift table.
    """
    print(f"Executing query: {query_str}")
    try:
        result = client_redshift.execute_statement(
            Database='dev',
            SecretArn=secret_arn,
            Sql=query_str,
            ClusterIdentifier=cluster_id
        )
        return result

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise
