import os
import json
import boto3
import botocore 
import botocore.session as bc
from botocore.client import Config
import time
 
print('Loading function')

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
    print("Entered lambda_handler")

    query_str = "create table public.call (id int);"
    
    try:
        result = client_redshift.execute_statement(Database= 'dev', SecretArn= secret_arn, Sql= query_str, ClusterIdentifier= cluster_id)
        print(f"API successfully executed :{result}")
        
    except botocore.exceptions.ConnectionError as e:
        client_redshift_1 = session.client("redshift-data", config = config)
        result = client_redshift_1.execute_statement(Database= 'dev', SecretArn= secret_arn, Sql= query_str, ClusterIdentifier= cluster_id)
        print("API executed after reestablishing the connection")
        return str(result)
        
    except Exception as e:
        raise Exception(e)
        
    return str(result)
    
def format_results(records, metadata):
    """Format the results into a list of dictionaries."""
    formatted = []
    column_names = [col["name"] for col in metadata]
    for record in records:
        row = {}
        for col_name, col_value in zip(column_names, record):
            if "stringValue" in col_value:
                row[col_name] = col_value["stringValue"]
            elif "longValue" in col_value:
                row[col_name] = col_value["longValue"]
            elif "doubleValue" in col_value:
                row[col_name] = col_value["doubleValue"]
            else:
                row[col_name] = None
        formatted.append(row)
    return formatted