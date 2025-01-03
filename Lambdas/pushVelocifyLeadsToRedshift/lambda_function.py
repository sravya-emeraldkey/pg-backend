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

def execute_redshift_query(query_str):
    """
    Execute a query in the Redshift cluster.
    """
    try:
        result = client_redshift.execute_statement(
            Database='dev',
            SecretArn=secret_arn,
            Sql=query_str,
            ClusterIdentifier=cluster_id
        )
        print(f"Query:{query_str}")
        print(f"Query executed successfully: {result}")
        return result
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise

def lambda_handler(event, context):
    print(f"Entered lambda_handler: {event}")
    event = {
        'bucket_name': 'raw-velocify-leads',
        'input_file_key': 'VelocifyLeads.csv',
        'output_file_key': 'processed/VelocifyLeads.csv',
        'table_name': 'public.staging_lead',
        'column_mapping': {
            'Id': 'Lead_ID',
            'Lead Source': 'Source',
            'Status': 'Lead_Status',
            'Lead Score #': 'Lead_Score',
            'Milestone': 'Milestone',
            'User':'Broker_Name',
            'Group': '"Group"',
            'Date Added': 'Date_Added',
            'Last Action': 'Last_Action',
            'First Contact Attempt Date': 'First_Contact_Attempt_Date',
            'Action Count': 'Action_Count',
            'Total Contact Attempts': 'Total_Contact_Attempts',
            'Last Action Date': 'Last_Action_Date',
            'First Assignment / Distribution Date': 'First_Assignment_Distribution_Date',
            'First Assignment / Distribution User': 'First_Assignment_Distribution_User',
            'Lead Source Group': 'Lead_Source_Group',
            'Creative': 'Creative',
            'Broker': 'Broker',
            'Opener': 'Opener',
            'IRA - Investment Dollar': 'IRA_Investment_Dollar',
            'Cash - Investment Dollar': 'Cash_Investment_Dollar',
            'Deal Type': 'Deal_Type',
            'Transfer Type': 'Transfer_Type',
            'TO Date': '"TO_Date"',
            'SF Lead ID': 'SF_Lead_ID',
            'Velocify ID': 'Velocify_ID',
            'Original Broker': 'Original_Broker',
            'SF Lead Owner': 'SF_Lead_Owner',
            'Junior Broker': 'Junior_Broker',
            'Last Activity': 'Last_Activity',
            'Intellect Client ID': 'Intellect_Client_ID',
            'Intellect Broker': 'Intellect_Broker',
            'First Name': 'First_Name',
            'Last Name': 'Last_Name',
            'Home Phone': 'Home_Phone',
            'Work Phone': 'Work_Phone',
            'Mobile Phone': 'Mobile_Phone',
            'Email': 'Email',
            'Secondary E-Mail': 'Secondary_Email',
            'Address': 'Address',
            'City': 'City',
            'State': 'State',
            'Zip/Postal Code': 'Zip_Postal_Code',
            'Source Code': 'Source_Code',
            'SubID': 'SubID'
        }

    }
    bucket_name = event['bucket_name']
    input_file_key = event['input_file_key']
    output_file_key = f"processed/{input_file_key}"
    table_name = event['table_name']
    column_mapping = event['column_mapping']
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
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Failed to process file: {str(e)}"
        }

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
        print(f'copy_query:{copy_query}')
        execute_redshift_query(copy_query)

        timezone_query = f"""
        SET timezone = 'US/Eastern';
        """
        execute_redshift_query(timezone_query)

        update_query = f"""
        UPDATE public.Lead
        SET 
            Source = staging.Source,
            Lead_Status = staging.Lead_Status,
            Lead_Score = staging.Lead_Score,
            Stage_ID = NULLIF(staging.Stage_ID, '')::INT,
            Milestone = staging.Milestone,
            Broker_Name = staging.Broker_Name,
            "Group" = staging."Group",
            Date_Added = COALESCE(NULLIF(staging.Date_Added, '')::TIMESTAMP, public.Lead.Date_Added),
            Last_Action = staging.Last_Action,
            First_Contact_Attempt_Date = COALESCE(NULLIF(staging.First_Contact_Attempt_Date, '')::TIMESTAMP, public.Lead.First_Contact_Attempt_Date),
            Action_Count = NULLIF(staging.Action_Count, '')::INT,
            Total_Contact_Attempts = NULLIF(staging.Total_Contact_Attempts, '')::INT,
            Last_Action_Date = COALESCE(NULLIF(staging.Last_Action_Date, '')::TIMESTAMP, public.Lead.Last_Action_Date),
            First_Assignment_Distribution_Date = COALESCE(NULLIF(staging.First_Assignment_Distribution_Date, '')::TIMESTAMP, public.Lead.First_Assignment_Distribution_Date),
            First_Assignment_Distribution_User = staging.First_Assignment_Distribution_User,
            Lead_Source_Group = staging.Lead_Source_Group,
            Creative = staging.Creative,
            Broker = staging.Broker,
            Opener = staging.Opener,
            IRA_Investment_Dollar = staging.IRA_Investment_Dollar,
            Cash_Investment_Dollar = REPLACE(REPLACE(NULLIF(staging.Cash_Investment_Dollar, ''), '$', ''), ',', '')::DECIMAL(15, 2),
            Deal_Type = staging.Deal_Type,
            Transfer_Type = staging.Transfer_Type,
            "TO_Date" = COALESCE(NULLIF(staging."TO_Date", '')::TIMESTAMP, public.Lead."TO_Date"),
            SF_Lead_ID = staging.SF_Lead_ID,
            Velocify_ID = staging.Velocify_ID,
            Original_Broker = staging.Original_Broker,
            SF_Lead_Owner = staging.SF_Lead_Owner,
            Junior_Broker = staging.Junior_Broker,
            Last_Activity = COALESCE(NULLIF(staging.Last_Activity, '')::TIMESTAMP, public.Lead.Last_Activity),
            Intellect_Client_ID = staging.Intellect_Client_ID,
            Intellect_Broker = staging.Intellect_Broker,
            First_Name = staging.First_Name,
            Last_Name = staging.Last_Name,
            Home_Phone = staging.Home_Phone,
            Work_Phone = staging.Work_Phone,
            Mobile_Phone = staging.Mobile_Phone,
            Email = staging.Email,
            Secondary_Email = staging.Secondary_Email,
            Address = staging.Address,
            City = staging.City,
            State = staging.State,
            Zip_Postal_Code = staging.Zip_Postal_Code,
            Source_Code = staging.Source_Code,
            SubID = staging.SubID
        FROM public.staging_lead AS staging
        WHERE public.Lead.Lead_ID = NULLIF(staging.Lead_ID, '')::INT;
        """

        execute_redshift_query(update_query)

        insert_query = f"""
        INSERT INTO public.Lead (
            Lead_ID, Source, Lead_Status, Lead_Score, Stage_ID, Milestone,Broker_Name, "Group", 
            Date_Added, Last_Action, First_Contact_Attempt_Date, Action_Count, Total_Contact_Attempts, 
            Last_Action_Date, First_Assignment_Distribution_Date, First_Assignment_Distribution_User, 
            Lead_Source_Group, Creative, Broker, Opener, IRA_Investment_Dollar, Cash_Investment_Dollar, 
            Deal_Type, Transfer_Type, "TO_Date", SF_Lead_ID, Velocify_ID, Original_Broker, SF_Lead_Owner, 
            Junior_Broker, Last_Activity, Intellect_Client_ID, Intellect_Broker, First_Name, Last_Name, 
            Home_Phone, Work_Phone, Mobile_Phone, Email, Secondary_Email, Address, City, State, 
            Zip_Postal_Code, Source_Code, SubID
        )
        SELECT DISTINCT
            CASE 
                WHEN NULLIF(staging.Lead_ID, '') IS NOT NULL THEN NULLIF(staging.Lead_ID, '')::INT
                ELSE 0 -- or handle with a default value for Lead_ID
            END AS Lead_ID,
            staging.Source,
            staging.Lead_Status,
            staging.Lead_Score,
            NULLIF(staging.Stage_ID, '')::INT AS Stage_ID,
            staging.Milestone,
            staging.Broker_Name,
            staging."Group",
            NULLIF(staging.Date_Added, '')::TIMESTAMP AS Date_Added,
            staging.Last_Action,
            NULLIF(staging.First_Contact_Attempt_Date, '')::TIMESTAMP AS First_Contact_Attempt_Date,
            NULLIF(staging.Action_Count, '')::INT AS Action_Count,
            NULLIF(staging.Total_Contact_Attempts, '')::INT AS Total_Contact_Attempts,
            NULLIF(staging.Last_Action_Date, '')::TIMESTAMP AS Last_Action_Date,
            NULLIF(staging.First_Assignment_Distribution_Date, '')::TIMESTAMP AS First_Assignment_Distribution_Date,
            staging.First_Assignment_Distribution_User,
            staging.Lead_Source_Group,
            staging.Creative,
            staging.Broker,
            staging.Opener,
            staging.IRA_Investment_Dollar,
            REPLACE(REPLACE(NULLIF(staging.Cash_Investment_Dollar, ''), '$', ''), ',', '')::DECIMAL(15, 2) AS Cash_Investment_Dollar,
            staging.Deal_Type,
            staging.Transfer_Type,
            NULLIF(staging."TO_Date", '')::TIMESTAMP AS "TO_Date",
            staging.SF_Lead_ID,
            staging.Velocify_ID,
            staging.Original_Broker,
            staging.SF_Lead_Owner,
            staging.Junior_Broker,
            NULLIF(staging.Last_Activity, '')::TIMESTAMP AS Last_Activity,
            staging.Intellect_Client_ID,
            staging.Intellect_Broker,
            staging.First_Name,
            staging.Last_Name,
            staging.Home_Phone,
            staging.Work_Phone,
            staging.Mobile_Phone,
            staging.Email,
            staging.Secondary_Email,
            staging.Address,
            staging.City,
            staging.State,
            staging.Zip_Postal_Code,
            staging.Source_Code,
            staging.SubID
        FROM public.staging_lead AS staging
        WHERE NULLIF(staging.Lead_ID, '') IS NOT NULL -- Ensures Lead_ID is not null
        AND NOT EXISTS (
            SELECT 1 FROM public.Lead WHERE public.Lead.Lead_ID = NULLIF(staging.Lead_ID, '')::INT
        );
        """
        # print(f"InsertQuery: {insert_query}")
        execute_redshift_query(insert_query)
        print("Data copied successfully to Redshift.")

        truncate_table_query_end = f"TRUNCATE TABLE {table_name};"
        print("Starting table truncation...-2")
        execute_redshift_query(truncate_table_query_end)  # Waits for completion
        print("Table truncated successfully.-2")
       
    except Exception as e:
        print(f"Error copying data to Redshift: {e}")
        raise

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

