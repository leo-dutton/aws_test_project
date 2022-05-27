# from curses import _CursesWindow
import boto3

from redshift_connect import create_cursor_for_redshift

cursor, conn = create_cursor_for_redshift()

# exapmle from docs
# ''' 
# copy table from 's3://<your-bucket-name>/load/key_prefix' 
# credentials 'aws_iam_role=arn:aws:iam::<aws-account-id>:role/<role-name>'
# options;
# '''

SCHEMA = 'public'
TABLE = '100_csv_staging'
AWS_FILE_PATH = 's3://awstrainingbkt/enrolment1/2020/month=01/myFile_17.csv'
# AWS_ACCOUNT_ID = '809192606572'
# AWS_ROLE = 'awstraining_s3_redshift_fullaccess'
AWS_CREDENTIALS = 'arn:aws:iam::809192606572:role/awstraining_s3_redshift_fullaccess'

query = f'COPY "{SCHEMA}"."{TABLE}" FROM \'{AWS_FILE_PATH}\' \
    CREDENTIALS \'aws_iam_role={AWS_CREDENTIALS}\' \
    csv IGNOREHEADER 1;'

cursor.execute(query)
conn.commit()
cursor.close()
conn.close()

cursor.execute(f'SELECT * FROM "{SCHEMA}"."{TABLE}";')
data = cursor.fetchall()

