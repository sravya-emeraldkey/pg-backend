import os
import json
import boto3
import io
import csv
from botocore.client import Config

# Initialize S3 client and Redshift client
s3_client = boto3.client('s3')
session = boto3.session.Session()
region = session.region_name

# Secrets Manager client
secret_name = os.environ['SecretId']
client_secretsmanager = session.client(service_name='secretsmanager', region_name=region)
get_secret_value_response = client_secretsmanager.get_secret_value(SecretId=secret_name)
secret_arn = get_secret_value_response['ARN']
secret_json = json.loads(get_secret_value_response['SecretString'])
cluster_id = secret_json['dbClusterIdentifier']

# Redshift client
config = Config(connect_timeout=5, read_timeout=5)
client_redshift = session.client("redshift-data", config=config)


def preprocess_csv(bucket_name, file_key, output_key, column_mapping):
    """
    Preprocess CSV: Rename columns based on `column_mapping` and filter unwanted columns.
    :param bucket_name: S3 bucket containing the input file.
    :param file_key: Key of the input CSV file in the bucket.
    :param output_key: Key for the processed CSV file in S3.
    :param column_mapping: Dict mapping CSV column names to Redshift table column names.
    """
    try:
        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        csv_file = io.StringIO(file_content)

        # Read and filter the CSV
        csv_reader = csv.DictReader(csv_file)
        processed_rows = []
        for row in csv_reader:
            processed_row = {new_col: row[old_col] for old_col, new_col in column_mapping.items() if old_col in row}
            processed_rows.append(processed_row)

        # Write the processed CSV to a new S3 key
        output_csv = io.StringIO()
        csv_writer = csv.DictWriter(output_csv, fieldnames=list(column_mapping.values()))
        csv_writer.writeheader()
        csv_writer.writerows(processed_rows)
        output_csv.seek(0)

        s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=output_csv.getvalue())
        print(f"Processed CSV uploaded to {output_key} in {bucket_name}")
    except Exception as e:
        print(f"Error preprocessing CSV: {e}")
        raise


def copy_data_to_redshift(s3_path, table_name, column_list):
    """
    COPY data from S3 to Redshift.
    :param s3_path: S3 path of the processed CSV.
    :param table_name: Target Redshift table name.
    :param column_list: List of target columns in Redshift table.
    """
    aws_access_key = os.environ['ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['SECRET_ACCESS_KEY']
    try:
    
        copy_query = f"""
        COPY {table_name} ({', '.join(column_list)})
        FROM '{s3_path}'
        CREDENTIALS 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_access_key}'
        CSV IGNOREHEADER 1;
        """
        execute_redshift_query(copy_query)
        insert_query_old = f"""
        INSERT INTO public.call (Call_ID, Lead_ID, Broker_Name, Outcome, Call_Segment, Call_Type, Date_Time, Talk_Time, Prospect_Number, Inbound_Number)
        SELECT DISTINCT Call_ID, Lead_ID, Broker_Name, Outcome, Call_Segment, Call_Type, Date_Time, Talk_Time, Prospect_Number, Inbound_Number
        FROM public.staging_call
        WHERE NOT EXISTS (
            SELECT 1 FROM public.call WHERE public.call.Call_ID = public.staging_call.Call_ID
        );
        """
        insert_query = f"""
        INSERT INTO public.call (
        Call_ID, Lead_ID, Broker_Name, Outcome, Call_Segment, Call_Type, Date_Time, Talk_Time, Prospect_Number, Inbound_Number
        )
        SELECT DISTINCT
            Call_ID,
            CASE
                WHEN Lead_ID ~ '^\d+$' THEN Lead_ID::INT  -- Valid integer Lead_ID
                ELSE NULL  -- Default invalid Lead_ID to NULL
            END AS Lead_ID,
            Broker_Name,
            Outcome,
            Call_Segment,
            Call_Type,
            CASE
                WHEN Date_Time = '' THEN NULL  -- Handle empty Date_Time
                ELSE NULLIF(Date_Time, '')::TIMESTAMP  -- Safely cast to TIMESTAMP
            END AS Date_Time,
            CASE
                WHEN Talk_Time ~ '^\d+:\d+:\d+$' THEN (
                    SPLIT_PART(Talk_Time, ':', 1)::INT * 3600 +  -- Parse HH
                    SPLIT_PART(Talk_Time, ':', 2)::INT * 60 +   -- Parse MM
                    SPLIT_PART(Talk_Time, ':', 3)::INT         -- Parse SS
                )
                WHEN Talk_Time ~ '^\d+:\d+$' THEN (
                    SPLIT_PART(Talk_Time, ':', 1)::INT * 60 +  -- Parse MM
                    SPLIT_PART(Talk_Time, ':', 2)::INT        -- Parse SS
                )
                WHEN Talk_Time ~ '^\d+$' THEN Talk_Time::INT   -- Handle raw integer durations
                ELSE NULL  -- Default invalid Talk_Time to NULL
            END AS Talk_Time,
            Prospect_Number,
            Inbound_Number
        FROM public.staging_call
        WHERE NOT EXISTS (
            SELECT 1 FROM public.call WHERE public.call.Call_ID = public.staging_call.Call_ID
        );

        """

        execute_redshift_query(insert_query)
        print("Data copied successfully to Redshift.")

        truncate_table_query_end = f"TRUNCATE TABLE {table_name};"
        print("Starting table truncation...-2")
        execute_redshift_query(truncate_table_query_end)  # Waits for completion
        print("Table truncated successfully.-2")
       
    except Exception as e:
        print(f"Error copying data to Redshift: {e}")
        raise



def lambda_handler(event, context):
    """
    Lambda function to process the CSV, preprocess, and load data into Redshift.
    """
    event = {
        'bucket_name': 'raw-velocify-calllogs',
        'input_file_key': 'Lm32481_CallHistory_20241119_211032_88636b0f-bece-47a5-9a5a-c2eed219a982.csv',
        'output_file_key': 'processed/Lm32481_CallHistory_20241119_211032_88636b0f-bece-47a5-9a5a-c2eed219a982.csv',
        'table_name': 'public.staging_call',
        'column_mapping': {
            'Call Id': 'Call_ID',
            'Lead Id': 'Lead_ID',
            'User': 'Broker_Name',
            'Result': 'Outcome',
            'Call Segment':'Call_Segment',
            'Origin':'Call_Type',
            'Time':'Date_Time',
            'Call Duration (hrs:min:sec)':'Talk_Time',
            'Prospect Number':'Prospect_Number',
            'Inbound Number':'Inbound_Number'
        }
    }
    
    
    # Input and output details
    bucket_name = event['bucket_name']
    input_file_key = event.file_key
    output_file_key = f"processed/{input_file_key}"
    table_name = 'public.staging_Call'

    # Column mapping: Map CSV column names to Redshift table column names
    column_mapping =  {
        'Call Id': 'Call_ID',
        'Lead Id': 'Lead_ID',
        'User': 'Broker_Name',
        'Result': 'Outcome',
        'Call Segment':'Call_Segment',
        'Origin':'Call_Type',
        'Time':'Date_Time',
        'Call Duration (hrs:min:sec)':'Talk_Time',
        'Prospect Number':'Prospect_Number',
        'Inbound Number':'Inbound_Number'
    }

    try:
        # Step 1: Truncate the staging table
        truncate_table_query = f"TRUNCATE TABLE {table_name};"
        print("Starting table truncation...-1")
        execute_redshift_query(truncate_table_query)  # Waits for completion
        print("Table truncated successfully.-1")

        # Step 2: Preprocess the CSV
        preprocess_csv(bucket_name, input_file_key, output_file_key, column_mapping)

        # Step 3: Construct S3 path for the processed CSV
        s3_path = f"s3://{bucket_name}/{output_file_key}"
        column_list = list(column_mapping.values())

        # Step 4: Load data into Redshift
        copy_data_to_redshift(s3_path, table_name, column_list)

        return {
            'statusCode': 200,
            'body': 'CSV processed and data loaded into Redshift successfully.'
        }
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': f"Error processing CSV: {str(e)}"
        }


def execute_redshift_query(query_str):
    """
    Execute a query in the Redshift cluster and wait for its completion.
    """
    try:
        # Execute the query
        response = client_redshift.execute_statement(
            Database='dev',
            SecretArn=secret_arn,
            Sql=query_str,
            ClusterIdentifier=cluster_id
        )
        statement_id = response['Id']
        print(f"Query submitted successfully. Statement ID: {statement_id}")

        # Wait for the query to complete
        while True:
            query_status = client_redshift.describe_statement(Id=statement_id)
            status = query_status['Status']
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            print(f"Waiting for query to complete... Current status: {status}")
        
        if status == 'FINISHED':
            print("Query executed successfully.")
            return query_status
        else:
            raise Exception(f"Query execution failed. Status: {status}, Error: {query_status.get('Error', 'Unknown error')}")

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise

