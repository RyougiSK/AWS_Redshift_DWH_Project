import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.

    Arguments:
    cur -- cursor object for the database connection
    conn -- database connection object

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.

    Arguments:
    cur -- cursor object for the database connection
    conn -- database connection object

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    1. Reads the configuration file 'dwh.cfg'
    2. Establishes database connection to the Redshift cluster
    3. Drops existing tables (if any)
    4. Creates new tables
    5. Closes the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop existing tables
    drop_tables(cur, conn)

    # Create new tables
    create_tables(cur, conn)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()