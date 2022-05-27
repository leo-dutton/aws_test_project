from curses import _CursesWindow
from webbrowser import get
import pandas as pd
import json

# If you need more information about configurations or implementing the sample code, visit the AWS docs:   
# https://aws.amazon.com/developers/getting-started/python/

import boto3
import base64
from botocore.exceptions import ClientError
from typing import Dict

import redshift_connector


SECRET_NAME = "arn:aws:secretsmanager:eu-west-2:809192606572:secret:awstraining_redshift1-rM4Mmy"

def get_secret(secret_name: str, region_name="eu-west-2") -> Dict:
    """_summary_

    Args:
        secret_name (str): name of the secret in AWS secrets manager.
        region_name (str, optional): _description_. Defaults to "eu-west-2".

    Returns:
        Dict: Contains secret credentials in key-value pairs as strings
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields
        # will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            
    secret = json.loads(secret)
    return secret


def connect_to_redshift():
    """Connects to redshift using the details and credentials acquired from 
    redshift_secret.py

    Returns:
        redshift_connector.core.Connection: connection to redshift database
    """
    secret = get_secret(SECRET_NAME)

    conn = redshift_connector.connect(
        host=secret['endpoint'],
        database=secret['database'],
        user=secret['user'],
        password=secret['password']
    )
    return conn

def create_cursor_for_redshift(return_conn=False):
    """Creates a cursor in redshift for executing sql queries.

    Returns:
        redshift_connector.cursor.Cursor: cursor within redshift, used to execute
        queries
    """
    conn = connect_to_redshift()
    cursor: redshift_connector.Cursor = conn.cursor()
    
    if return_conn:
        return cursor, conn
    else:
        return cursor

def execute_sql_query_in_redshift(query: str):
    """Executes the query input in redshift and returns a table of results 

    Args:
        query (str): postgres sql query for redshift

    Returns:
        tuple: data returned from query
    """
    cursor, conn = create_cursor_for_redshift(return_conn=True)
    cursor.execute(query)
    data = cursor.fetchall()
    return data

