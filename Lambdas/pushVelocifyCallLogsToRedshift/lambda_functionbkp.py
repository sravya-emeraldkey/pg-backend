import os
import json
import boto3
import botocore
import io
import csv
import time
from datetime import datetime
from botocore.client import Config

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
    # file_key = event['file_key']
    file_key = "Lm32481_CallHistory_20241119_205339_b1af15ef-42cb-4b8a-b8f4-61dd443a0204.csv"
    # bucket_name = ""
    bucket_name = event['bucket_name']
    start_index = event.get('start_index', 0)
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

            # Handle missing or optional fields
            call_id = row.get('Call Id') or 'NULL'
            lead_id = row.get('Lead Id') or 'NULL'
            origin = row.get('Origin') or 'NULL'
            outcome = row.get('Result') or 'NULL'
            call_segment = row.get('Call_Segment') or 'NULL'
            prospect_number = row.get('Prospect_Number') or 'NULL'
            inbound_number = row.get('Inbound_Number') or 'NULL'
            broker_name = row.get('User') or 'NULL'
            # print(f"Broker name: {broker_name}")
            if ',' in broker_name:
                last_name, first_name = map(str.strip, broker_name.split(','))
                normalized_broker_name = f"{first_name} {last_name}"
                get_broker_sql_query = f"""
                    SELECT broker_id FROM public.broker WHERE broker_name ILIKE '%{normalized_broker_name}%';
                    """
                broker_result = execute_redshift_query_to_fetch_results(get_broker_sql_query)
            else:
                # print(f"Null case: {broker_name}")
                broker_result =[]
            # print(f"broker_result: {broker_result}")
            broker_id = broker_result[0] if broker_result else 'NULL'
            time_field = row.get('Time') or 'NULL'
            call_duration = row.get('Call Duration (hrs:min:sec)', '0:00:00')
            talk_time = duration_to_seconds(call_duration) or 'NULL'

            # Wrap strings in single quotes, use NULL for missing values
            time_field = f"'{time_field}'" if time_field != 'NULL' else time_field
            call_id = f"'{call_id}'" if call_id != 'NULL' else call_id
            broker_id = f"'{broker_id}'" if broker_id != 'NULL' else broker_id
            origin = f"'{origin}'" if origin != 'NULL' else origin
            outcome = f"'{outcome}'" if outcome != 'NULL' else outcome
            call_segment = f"'{call_segment}'" if call_segment != 'NULL' else call_segment
            prospect_number = f"'{prospect_number}'" if prospect_number != 'NULL' else prospect_number
            inbound_number = f"'{inbound_number}'" if inbound_number != 'NULL' else inbound_number


            try:
                # Construct SQL queries to delete and insert
                delete_sql_query = f"""
                DELETE FROM public.Call WHERE Call_ID = {call_id};
                """
                insert_sql_query = f"""
                INSERT INTO public.Call (Call_ID, Call_Platform, Lead_ID, Call_Type, Date_Time, Talk_Time, Broker_ID,Outcome,Call_Segment,Prospect_Number,Inbound_Number)
                VALUES ({call_id}, 'Velocify', {lead_id}, {origin}, {time_field}, {talk_time}, {broker_id},{outcome},{call_segment},{prospect_number},{inbound_number});
                """
                lead_check_insert_query = f"""
                INSERT INTO public.Lead (Lead_ID, Source, Lead_Status, Lead_Score, Creation_Date)
                SELECT {lead_id}, 'Velocify', NULL, NULL, {time_field}
                WHERE NOT EXISTS (
                    SELECT 1 FROM public.Lead WHERE Lead_ID = {lead_id}
                );
                """

                # Execute delete query first
                execute_redshift_query(delete_sql_query)

                # Execute insert query
                execute_redshift_query(insert_sql_query)

                # Execute check and insert query
                execute_redshift_query(lead_check_insert_query)

            except Exception as row_error:
                print(f"Error processing row {row}: {str(row_error)}")

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
    # print(f"Executing query: {query_str}")
    try:
        result = client_redshift.execute_statement(
            Database='dev',
            SecretArn=secret_arn,
            Sql=query_str,
            ClusterIdentifier=cluster_id
        )
        # print(f"Query executed successfully: {result}")
        return result

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise

def duration_to_seconds(duration_str):
    """
    Convert a duration string in the format hrs:min:sec to seconds.
    """
    try:
        h, m, s = map(int, duration_str.split(":"))
        total_seconds = h * 3600 + m * 60 + s
        return total_seconds
    except ValueError:
        print(f"Invalid duration format: {duration_str}")
        return None

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
def execute_redshift_query_to_fetch_results(query_str):
    """
    Execute a query in the Redshift cluster, check its status, and fetch results.
    """
    # print(f"Executing query: {query_str}")
    try:
        # Execute the query
        response = client_redshift.execute_statement(
            Database='dev',
            SecretArn=secret_arn,
            Sql=query_str,
            ClusterIdentifier=cluster_id
        )
        query_id = response['Id']
        print(f"Query executed successfully. Query ID: {query_id}")
        
        # Check query status
        while True:
            query_status = client_redshift.describe_statement(Id=query_id)
            status = query_status['Status']
            if status in ['FINISHED']:
                break
            elif status in ['FAILED', 'ABORTED']:
                raise Exception(f"Query failed with status: {status}. Error: {query_status.get('Error', 'Unknown error')}")
            else:
                print(f"Query status: {status}. Waiting for completion...")
                time.sleep(2)  # Wait for 2 seconds before rechecking
        
        # Fetch results using the query ID
        result = client_redshift.get_statement_result(Id=query_id)
        records = result.get('Records', [])
        
        # Process and return the result
        if records:
            # Assuming the first column contains the broker_id
            broker_ids = [record[0]['stringValue'] for record in records]
            # print(f"Query results: {broker_ids}")
            return broker_ids
        else:
            print("No records found.")
            return None

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise

        print(f"Error executing query: {str(e)}")
        raise
