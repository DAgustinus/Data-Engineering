# Airflow

ETL Pipeline process for:
1. Create Staging and Production table
2. Copy S3 data to staging table
3. Clean up the data and load them to Facts and Dimensional tables


## What's in this section?

Three main folders:
1. dags - this folder consists of the main DAG that would run the data pipeline
2. [data_config](https://github.com/DAgustinus/Data-Engineering/blob/master/Data%20Pipeline%20-%20Airflow/data_config/sql_config.txt) - this folder consists of an easy sql_config.txt file which determines how the tables are supposedly created
3. plugins - this folder holds all of the operators and helpers which help set the pipeline

## Contact information:

- [LinkedIn](https://www.linkedin.com/in/dagustinus/)
