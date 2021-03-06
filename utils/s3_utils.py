from asyncore import read
import os
import re
import boto3
import pandas as pd

_S3_PATH = 's3://'


def create_aws_file_path(bucket: str, aws_file_path: str):
    """Joins the s3 path, bucket name, and path to file into a single string

    Args:
        aws_file_path (str): path to file within s3 bucket

    Returns:
        str: full path of file stored on s3
    """
    # full_path = os.path.join(_S3_PATH, _BUCKET, aws_file_path)
    full_aws_file_path = _S3_PATH + bucket + aws_file_path
    return full_aws_file_path

def read_csv_from_aws(aws_file_path: str):
    """Loads the csv file specified from aws into a pandas df

    Args:
        aws_file_path (str): path to file within s3 bucket

    Returns:
        df: dataframe of csv file loaded from s3
    """
    full_path = create_aws_file_path(aws_file_path)
    data = pd.read_csv(full_path)

    return data

if __name__=='__main__':
    data = read_csv_from_aws('/enrolment1/2020/month=01/myFile_17.csv')
    print(data)