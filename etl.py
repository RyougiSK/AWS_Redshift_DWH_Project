import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3


def load_staging_tables(cur, conn):
    '''
    This function is used to copy data from staging tables to Redshift.

    Parameters:
    cur (cursor): psycopg2 cursor object for executing SQL queries.
    conn (connection): psycopg2 connection object to Redshift database.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This function is used to insert data into fact and dimension tables from staging tables.

    Parameters:
    cur (cursor): psycopg2 cursor object for executing SQL queries.
    conn (connection): psycopg2 connection object to Redshift database.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    This function is used to connect to the Redshift cluster, load data from S3 to Redshift
    staging tables, and then insert data into fact and dimension tables.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load data from S3 to Redshift staging tables
    load_staging_tables(cur, conn)

    # Insert data into fact and dimension tables
    insert_tables(cur, conn)

    conn.close()

    # An alert that remind the etl process has been completed
    print('ETL process has been successfully completed')


if __name__ == "__main__":
    main()