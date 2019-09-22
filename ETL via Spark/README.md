# Summary
### The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository

Thank you for spending the time to take a look at the README.md file!

#### This readme file will go through:
1. The files within this projects
2. The available Python script
3. How to run the scripts
4. Overview of the "Data Lake using Spark.ipynb file"
    
This project is based on the Udacity Data Engineering course and it teaches the student to perform an ETL using Spark and automating it by using Python.

#### In this project, we have:
1. One Python Notebook 
2. One Python files
3. Config file which is used to get all of the proper data dl.cfg
    
The Python Notebook is used to test the ETL as well as to run SQL test to ensure that the data are inside of the tables that we have created. As for the Python file, they are used to wrangle the data from S3 or Local, store all of the SQL queries, as well as the main ETL process. Last but not least, the Data folder, consists of layers of data that is stored in json format.

To start seeing the results, go ahead and follow the steps below:
1. go to the dl.cfg and enter in your AWS Access key and the Secret key
2. Open the etl.py and you will be available to adjust the destination of the S3 and source of the data 
3. Run the etl.py script in terminal by entering 'Python3 etl.py'

Thanks for checking it out!

*Dan Agustinus*
