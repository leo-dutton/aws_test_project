from pkgutil import get_data
from utils.redshift_utils import \
    create_cursor_for_redshift, \
    execute_sql_query_in_redshift, \
    commit_connection, \
    close_connection_and_cursor
from utils.sql_queries import \
    query_to_copy_s3_to_redshift
from utils.s3_utils import create_aws_file_path
# from utils.s3_utils import read_csv_from_aws

_BUCKET = 'awstrainingbkt'
_FILE_PATH = '/enrolment1/2020/month=01/myFile_17.csv'

_SCHEMA = 'public'
_TABLE = '100_csv_staging'


def main() -> None:
    aws_file_path = create_aws_file_path(_BUCKET, _FILE_PATH)

    cursor, conn = create_cursor_for_redshift()

    query = query_to_copy_s3_to_redshift(_SCHEMA, _TABLE, aws_file_path)

    execute_sql_query_in_redshift(query, cursor)

    commit_connection(conn)

    close_connection_and_cursor(cursor, conn)

if __name__=='__main__':
    main()


# read_csv_from_aws('/enrolment1/2020/month=01/myFile_17.csv')