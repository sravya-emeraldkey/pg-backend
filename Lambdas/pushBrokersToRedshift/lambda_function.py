import os
import json
import boto3
import botocore 
import botocore.session as bc
from botocore.client import Config
import time
import io
import uuid


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
    brokers = brokers_list()
    try:
        for broker in brokers :
            try:
                # Handle optional fields and substitute NULL where necessary
                broker_id = uuid.uuid4()
                broker_name = broker.get('name') or 'NULL'
                phoneNumber = broker.get('phoneNumber') or 'NULL'
                extension = broker.get('extension') or 'NULL'
                email = broker.get('email') or 'NULL'
                role = broker.get('role') or 'NULL'
                state = broker.get('state') or 'NULL'
                broker_tenure = broker.get('broker_tenure') or 'NULL'

                # If the fields are strings, wrap them in single quotes, otherwise use raw values
                broker_id = f"'{broker_id}'" if broker_id != 'NULL' else broker_id
                broker_name = f"'{broker_name}'" if broker_name != 'NULL' else broker_name
                phoneNumber = f"'{phoneNumber}'" if phoneNumber != 'NULL' else phoneNumber
                email = f"'{email}'" if email != 'NULL' else email
                role = f"'{role}'" if role != 'NULL' else role
                state = f"'{state}'" if state != 'NULL' else state
                extension = f"'{extension}'" if extension != 'NULL' else extension
                # broker_tenure = f"'{broker_tenure}'" if broker_tenure != 'NULL' else broker_tenure
                delete_sql_query = f"""
                DELETE FROM public.Broker WHERE broker_name = {broker_name};
                """
                # Construct the SQL query
                insert_sql_query = f"""
                INSERT INTO public.Broker (broker_id, broker_name,phoneNumber,email,extension,state,role)
                VALUES ({broker_id}, {broker_name},{phoneNumber},{email},{extension},{state},{role});
                """
                # Execute delete query first
                execute_redshift_query(delete_sql_query)
                
                # Execute insert query
                execute_redshift_query(insert_sql_query)
            
            except Exception as broker_error:
                print(f"Error processing broker {broker}: {str(broker_error)}")
        
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


def brokers_list():
  return [
  {
    "name": "Alan Austin",
    "phoneNumber": "469-334-2009",
    "extension": "175",
    "email": "a.austin@prioritygold.com",
    "state": "R",
    "role": "Jr. Account Executive",
  },
  {
    "name": "Alex Castle",
    "phoneNumber":"469-507-2483",
    "extension": "140",
    "email": "a.castle@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Allen James",
    "phoneNumber":"469-902-7344",
    "extension": "181",
    "email": "a.james@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Andrew Aragon",
    "phoneNumber":"469-729-8489",
    "extension": "115",
    "email": "a.aragon@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "BJ Slack",
    "phoneNumber":"469-613-0587",
    "extension": "141",
    "email": "b.slack@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Brandon Means",
    "phoneNumber":"469-775-9578",
    "extension": "189",
    "email": "b.means@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Brandon Perkins",
    "phoneNumber":"469-405-3454",
    "extension": "173",
    "email": "b.perkins@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Brian Dow",
    "phoneNumber":"469-327-2202",
    "extension": "158",
    "email": "b.dow@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Chris Cox",
    "phoneNumber":"469-507-3592",
    "extension": "151",
    "email": "c.cox@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Daniel Ross",
    "phoneNumber":"469-935-8938",
    "extension": "130",
    "email": "d.ross@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "David Waters",
    "phoneNumber":"469-935-6628",
    "extension": "121",
    "email": "d.waters@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Gregory Matthews",
    "phoneNumber":"469-868-8645",
    "extension": "116",
    "email": "g.matthews@prioritygold.com",
    "state":"TX",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Hayden Rosene",
    "phoneNumber":"469-405-2874",
    "extension": "191",
    "email": "hayden.r@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Johanna Trejo",
    "phoneNumber":"469-453-5735",
    "extension": "156",
    "email": "johanna.t@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Jon Wise",
    "phoneNumber":"469-729-8270",
    "extension": "185",
    "email": "j.wise@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Josh Johnson",
    "phoneNumber":"469-430-0791",
    "extension": "150",
    "email": "josh.j@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Lawrence Waters",
    "phoneNumber":"469-405-7330",
    "extension": "186",
    "email": "lawrence.w@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Liam Kildare",
    "phoneNumber":"469-947-6037",
    "extension": "165",
    "email": "l.kildare@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Lindsey Banghart",
    "phoneNumber":"469-405-3539",
    "extension": "188",
    "email": "l.banghart@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Luke Green",
    "phoneNumber":"469-902-9314",
    "extension": "118",
    "email": "l.green@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Matt Arrieta",
    "phoneNumber":"469-902-6762",
    "extension": "135",
    "email": "m.arrieta@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Matthew Boylan",
    "phoneNumber":"469-484-4098",
    "extension": "136",
    "email": "m.boylan@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Michael Flores",
    "phoneNumber":"469-895-2879",
    "extension": "190",
    "email": "m.flores@prioritygold.com",
    "state":"TX",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Ryan Daniels",
    "phoneNumber":"469-613-0258",
    "extension": "137",
    "email": "r.daniels@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Scott Wagner",
    "phoneNumber":"469-405-1606",
    "extension": "193",
    "email": "s.wagner@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Sheri Nassery",
    "phoneNumber":"469-729-8874",
    "extension": "113",
    "email": "s.nassery@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Taylor Millsap",
    "phoneNumber":"469-405-7906",
    "extension": "183",
    "email": "t.millsap@prioritygold.com",
    "state":"TX",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Todd Cook",
    "phoneNumber":"469-656-3839",
    "extension": "126",
    "email": "t.cook@prioritygold.com",
    "state":"TX",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Tom Hogan",
    "phoneNumber":"469-902-6773",
    "extension": "120",
    "email": "tom.h@prioritygold.com",
    "state":"R",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Tom Sullivan",
    "phoneNumber":"469-351-2700",
    "extension": "157",
    "email": "t.sullivan@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Anthony Brancieri",
    "phoneNumber":"469-902-8549",
    "extension": "180",
    "email": "a.brancieri@prioritygold.com",
    "state":"R",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Barbie Oyama",
    "phoneNumber":"469-694-8551",
    "extension": "134",
    "email": "b.oyama@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Bill Francis",
    "phoneNumber":"469-458-6164",
    "extension": "147",
    "email": "b.francis@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Bill Goocher",
    "phoneNumber":"469-902-7325",
    "extension": "128",
    "email": "b.goocher@prioritygold.com",
    "state":"R",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Eric Schwartz",
    "phoneNumber":"469-405-1565",
    "extension": "192",
    "email": "e.schwartz@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Jim Lairmore",
    "phoneNumber":"469-902-8915",
    "extension": "117",
    "email": "j.lairmore@prioritygold.com",
    "state":"R",
    "role":"Sr. Account Executive",
  },
  {
    "name": "John Anthony",
    "phoneNumber":"469-405-6395",
    "extension": "176",
    "email": "j.anthony@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Jonathan Carrington",
    "phoneNumber":"469-334-2062",
    "extension": "178",
    "email": "j.carrington@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Jonathan Wagner",
    "phoneNumber":"469-802-7068",
    "extension": "194",
    "email": "j.wagner@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Kwan Kim",
    "phoneNumber":"469-663-5421",
    "extension": "171",
    "email": "k.kim@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Mark Hamilton",
    "phoneNumber":"469-421-9099",
    "extension": "144",
    "email": "m.hamilton@prioritygold.com",
    "state":"CA",
    "role":"Jr. Account Executive",
  },
  {
    "name": "Michael Clouse",
    "phoneNumber":"469-895-2806",
    "extension": "109",
    "email": "m.clouse@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Michael Perelman",
    "phoneNumber":"469-663-5526",
    "extension": "167",
    "email": "m.perelman@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Morgan Steckler",
    "phoneNumber":"469-466-5648",
    "extension": "108",
    "email": "m.steckler@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Reginald Flowers",
    "phoneNumber":"469-663-5679",
    "extension": "168",
    "email": "r.flowers@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Richard Stites",
    "phoneNumber":"469-466-5662",
    "extension": "110",
    "email": "richard.s@prioritygold.com",
    "state":"R",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Steve Mitchell",
    "phoneNumber":"469-405-8121",
    "extension": "182",
    "email": "s.mitchell@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Steven Bruce",
    "phoneNumber":"469-663-5799",
    "extension": "169",
    "email": "s.bruce@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Terry Kelly",
    "phoneNumber":"469-210-1687",
    "extension": "129",
    "email": "t.kelly@prioritygold.com",
    "state":"CA",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Tony Magana",
    "phoneNumber":"469-648-3897",
    "extension": "146",
    "email": "t.magana@prioritygold.com",
    "state":"R",
    "role":"Sr. Account Executive",
  },
  {
    "name": "Justin Hightower",
    "phoneNumber": "469-436-6962",
    "extension": "107",
    "email": "j.hightower@prioritygold.com",
    "state": "CA",
    "role": "Sr. Account Executive",
  },
  {
    "name": "Brett Rasic",
    "phoneNumber": "469-942-6556",
    "extension": "133",
    "email": "b.rasic@prioritygold.com",
    "state": "CA",
    "role": "Jr. Account Executive",
  }
]

