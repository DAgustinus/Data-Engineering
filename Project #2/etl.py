# Import Python packages 
import cassandra
from cassandra.cluster import Cluster
import os
import glob
import csv
from sql_queries import *
import time
from datetime import datetime

"""
    At the top, we have imported some of the most important libraries:
    1. os -- this helps us to navigate through the necessary folders... Think of it as your ls/cd/mkdir options in your terminal
    2. glob -- this helps us to gather all of the file names and put it into a list
    3. cassandra -- the necessary connection to the NoSQL DB was created using this library
    4. sql_queries -- this is actually an import of our very own sql_queries.py. We passed all of our queries by doing this
"""

def process_data(session):
    
    """
    Description: This function is used to process the data that has been aggregated and pushed them into the tables
    
    Arguments:
        session: the session object we used to commit the queries 
        
    Returns:
        None
    """
    
    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + "Processing Data") 
    
    # We pull all of the data that needs to be uploaded here
    file = 'event_datafile_new.csv'
    
    # We open the file, and read every single line. We have the session.execute for each line to push all of the data 
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            try:
                session.execute(insert_music_session_items, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
            except Exception as e:
                print(e, " -- music_session_items")
                break
            
            try:
                session.execute(insert_user_sessions, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
            except Exception as e:
                print(e, " -- user_sessions")
                break
            
            try:
                session.execute(insert_song_sessions, (int(line[10]), line[9], line[1], line[4]))
            except Exception as e:
                print(e, " -- song_sessions")
                break

    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + "All data has been uploaded -- go ahead and check Test_query.ipynl to test the results!")

def aggregate_files():

    """
    Description: 
        This function is used to aggregate all of the individual/daily files into one.
        It is a lot easier for the process to run all of the files at once, but it might 
        be better to do it one by one if there are larger amount.
    
    Arguments:
        None
        
    Returns:
        None
        
    What it produce:
        A csv file called event_datafile_new.csv
    """
    
    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + "Starting Aggregation of data")
    
    full_data_rows_list = []
    
    filepath = os.getcwd() + '/event_data'
    
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*.csv'))
    
    # Get total number of files in the event_agenda
    num_files = len(file_path_list)
    
    # for every filepath in the file path list 
    for num, f in enumerate(file_path_list, 1):
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

            # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line)
        
        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + '{}/{} files processed.'.format(num, num_files))

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
    print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + "Aggregation of data has been completed")

def connect_to_keyspace():
    
    """
    Description: 
        This function is used to connect to the keyspace
    
    Arguments:
        None
        
    Returns:
        session & cluster
    
    """
    
    try:
        cluster = Cluster(['127.0.0.1'])
        # To establish connection and begin executing queries, need a session
        session = cluster.connect()
    except Exception as e:
        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + "!!!!!! There's something wrong while trying to connect to the session")
        print(e)
        
    try:
        session.set_keyspace('udacity')
        return session, cluster
    except Exception as e:
        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + "!!!!!! There's something wrong while trying to set the keyspace")
        print(e)
    
        
def main():
    
    """
    #1 The main starts by getting the session and cluster objects and returns it to a variable within this function
    #2 Once the connection is established, we start aggregating the data by running aggregate_files() to combine the 
       individual files into 1 single file
    #3 From there, we run the process_data() by passing session so we can execute the inserts for each of the lines
       within the aggregated CSV
    """
    
    start_time = time.time()
    
    print("\n" + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " -- " + "Starting Ingestion")
    
    session, cluster = connect_to_keyspace()
    
    aggregate_files()
    
    process_data(session)
    
    session.shutdown()
    cluster.shutdown()
    print("\n### Total Run time: %s seconds\n" % (time.time() - start_time))

if __name__ == "__main__":
    main()
