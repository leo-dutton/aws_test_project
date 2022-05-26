import redshift_connector
import pandas as pd

from redshift_secret import secret

DATABASE = secret['database']
USER = secret['user']
CLUSTER_ID = secret['cluster_identifier']
REGION = secret['region']
ENDPOINT = secret['endpoint']
PASSWORD = secret['password']
ARN = secret['arn']
PORT = secret['port']
SCHEMA = secret['schema']

def connect_to_redshift():
    """Connects to redshift using the details and credentials acquired from 
    redshift_secret.py

    Returns:
        redshift_connector.core.Connection: connection to redshift database
    """
    conn = redshift_connector.connect(
        host=ENDPOINT,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )
    return conn

def create_cursor_for_redshift():
    """Creates a cursor in redshift for executing sql queries.

    Returns:
        redshift_connector.cursor.Cursor: cursor within redshift, used to execute
        queries
    """
    conn = connect_to_redshift()
    cursor: redshift_connector.Cursor = conn.cursor()
    return cursor

def execute_sql_query_in_redshift(query: str):
    """Executes the query input in redshift and returns a table of results 

    Args:
        query (str): postgres sql query for redshift

    Returns:
        tuple: data returned from query
    """
    cursor = create_cursor_for_redshift()
    cursor.execute(query)
    data = cursor.fetchall()
    return data

query = 'select * from "public"."100_csv_staging"'
data = execute_sql_query_in_redshift(query)
data = pd.DataFrame(data)

print(data)
