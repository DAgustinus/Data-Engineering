import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

        
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        
def main():
    
    # We start the process by running the configparser to get all of the necessary configuration information
    # Pyscopg2 is then used to connect to the cluster in AWS
    # We use the drop_tables() and the create_tables() to drop/create the necessary tables
    # All of the queries are within the sql_queries.py
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    print('Tables has been dropped.')
    create_tables(cur, conn)
    print('All tables has been created.')
    conn.close()


if __name__ == "__main__":
    main()