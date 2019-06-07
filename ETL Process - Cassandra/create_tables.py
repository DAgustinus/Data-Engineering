import cassandra
from cassandra.cluster import Cluster
from sql_queries import *
from datetime import datetime


def create_database():
    # connect to default database
    try:
        cluster = Cluster(['127.0.0.1'])
        # To establish connection and begin executing queries, need a session
        session = cluster.connect()
    except Exception as e:
        print(e)
    
    # TO-DO: Create a Keyspace 
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity
        WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}
        """)
    except Exception as e:
        print(e)

    # TO-DO: Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)
    
    return session, cluster


def drop_tables(session):
    for query in drop_table_queries:
        session.execute(query)


def create_tables(session):
    for query in create_table_queries:
        session.execute(query)


def main():
    session, cluster = create_database()
    
    drop_tables(session)
    create_tables(session)
    
    
    print("\n" + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + 'Tables has been created')
    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + 'Session and Clusters are closed')

if __name__ == "__main__":
    main()