# Airflow

ETL Pipeline process for:
1. Create Staging and Production table
2. Copy S3 data to staging table
3. Clean up the data and load them to Facts and Dimensional tables


## Requirements:
Please make sure that your Airflow connection have the following connections below. The ones in bold needs to be named in that way to make sure that everything runs perfectly. All other ones that are not in bold are informations depending on your AWS:

1. Set up Connection called _**aws_credentials**_ which consists:
    * Conn Id:    _**aws_credentials**_
    * Conn Type:  _**Amazon Web Services**_
    * Login:      Your AWS Key
    * Password:   Secret Key
    
2. Set up Connection called _**redshift**_ which consists of:
    * Conn Id:    _**redshift**_
    * Conn Type:  _**Postgres**_
    * Host:       link to your cluster
    * Schema:     The DB
    * Login:      The user name to access the DB
    * Password:   The password to the DB
    * Port:       _**5439**_


## What's in this section?

Three main folders:
1. dags - this folder consists of the main DAG that would run the data pipeline
2. [data_config](https://github.com/DAgustinus/Data-Engineering/blob/master/Data%20Pipeline%20-%20Airflow/data_config/sql_config.txt) - this folder consists of an easy sql_config.txt file which determines how the tables are supposedly created
3. plugins - this folder holds all of the operators and helpers which help set the pipeline


## The pipeline DAG
![pipeline](https://github.com/DAgustinus/Data-Engineering/blob/master/Data%20Pipeline%20-%20Airflow/illustrations-non-codes/pipeline_dag.JPG?raw=true)


## Contact information:

- [LinkedIn](https://www.linkedin.com/in/dagustinus/)
