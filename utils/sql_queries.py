from utils.redshift_utils import get_secret, SECRET_NAME


def query_to_select_all_from_redshift(schema: str, table: str) -> str:
    """Creates a sql query string to select all rows from a specified schema and table

    Args:
        schema (str): desired schema name for the databse in redshift
        table (str): desired table name for the database in redshift

    Returns:
        str: sql query, "select * from xxx.xxx"
    """
    query = f'select * from "{schema}"."{table}"'
    
    return query

def query_to_copy_s3_to_redshift(schema: str, table: str, aws_file_path: str) -> str:
    """Creates a sql query string to copy data from a specified file in s3 to a 
    specified schema and table in redshift

    Args:
        schema (str): desired schema name for the databse in redshift
        table (str): desired table name for the database in redshift
        aws_file_path (str): _description_

    Returns:
        str: sql query, copy from s3 file to specified schema and table in redshift
    """
    secret = get_secret(SECRET_NAME)
    aws_credentials = secret['arn']

    query = f'COPY "{schema}"."{table}" FROM \'{aws_file_path}\' \
    CREDENTIALS \'aws_iam_role={aws_credentials}\' \
    csv IGNOREHEADER 1;'

    return query