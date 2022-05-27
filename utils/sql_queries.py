

def query_to_copy_s3_to_redshift(schema, table, aws_file_path):
    
    secret = get_secret(SECRET_NAME)
    aws_credentials = secret['arn']

    query = f'COPY "{schema}"."{table}" FROM \'{aws_file_path}\' \
    CREDENTIALS \'aws_iam_role={aws_credentials}\' \
    csv IGNOREHEADER 1;'