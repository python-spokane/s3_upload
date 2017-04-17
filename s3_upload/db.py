# Python Imports
import csv
import pymysql

# App Imports
from .s3_writer import S3StreamWriter


def export_db_table(bucket_name, table_name, db_connection):
    key_name = '%s.csv' % table_name
    s3_kwargs = {
        'ServerSideEncryption': 'AES256',
        'StorageClass': 'REDUCED_REDUNDANCY'
    }

    with S3StreamWriter(bucket_name, key_name, **s3_kwargs) as file_obj:
        writer = csv.writer(file_obj)

        with db_connection.cursor() as cursor:
            sql = 'SHOW COLUMNS FROM `%s`' % table_name
            cursor.execute(sql)

            columns = [col[0] for col in cursor]
            writer.writerow(columns)

        with db_connection.cursor() as cursor:
            sql = 'SELECT * FROM `%s`' % table_name
            cursor.execute(sql)

            for row in cursor:
                writer.writerow(row)


def export():
    conn_kwargs = {
        'host': 'localhost',
        'user': 'root',
        'password': 'root',
        'database': 'TestDb',
        'port': 3306,
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.SSCursor,
    }

    db_connection = pymysql.connect(**conn_kwargs)

    export_db_table('steve.2ndwatch.com', 'TestTable', db_connection)
