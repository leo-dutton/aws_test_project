import os
import boto3
import pandas as pd


url = 'awstrainingbkt/enrolment1/2020/month=01/myFile_17.csv'

data = pd.read_csv('s3://'+url)

print(data.head())