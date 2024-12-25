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

    query_str = """
    SELECT * FROM "dev"."public"."stage";
    """
    
    try:
        result = client_redshift.execute_statement(Database= 'dev', SecretArn= secret_arn, Sql= query_str, ClusterIdentifier= cluster_id)
        print(f"API successfully executed :{result}")
        query_id = result['Id']
        print(f"Query ID: {query_id}")
        status = None
        while status not in ["FINISHED", "FAILED", "ABORTED"]:
            query_status = client_redshift.describe_statement(Id=query_id)
            status = query_status["Status"]
            print(f"Query Status: {status}")
            if status in ["FAILED", "ABORTED"]:
                raise Exception(f"Query {status}: {query_status.get('Error')}")
            time.sleep(2)  # Wait 2 seconds before checking again

        statement_result  = client_redshift.get_statement_result(Id=query_id)
        records = statement_result ["Records"]
        final_results = format_results(records, statement_result["ColumnMetadata"])
        print(f"Records: {final_results}")
        
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