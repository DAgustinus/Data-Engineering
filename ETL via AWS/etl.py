import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_tables import *


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    # We start the process by running the configparser to get all of the necessary configuration information
    # Pyscopg2 is then used to connect to the cluster in AWS
    # We use the load_staging_tables() to load the raw data from S3 to staging table
    # Once it's loaded, we transformed the data and load the rest to the Production table by using the insert_tables() 
    # All of the queries are within the sql_queries.py
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()