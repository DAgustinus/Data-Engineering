The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository

Thank you for spending the time to take a look at the README.md file!

This readme file will go through:
    1. the files within this projects
    2. the available Python scripts
    3. how to run the scripts
    
This project is based on the Udacity Data Engineering course and it teaches the student to create Postgres database from scratch and also add the necessary columns, primary key, and even constraints. 

In this project, we have:
    1. Two Python Notebooks
    2. Three Python files
    3. Data folder which the data can be found
    
The two Python Notebooks are used to test the ETL as well as to run SQL test to ensure that the data are inside of the tables that we have created. As for the three Python files, they are used to create the necessary tables, store all of the SQL queries, as well as the main ETL process. Last but not least, the Data folder, consists of layers of data that is stored in JSON.

To start seeing the results, go ahead and run create_tables.py script. This will create all of the necessary tables and columns. Once you do that, you have two options:
    1. Run everything all at once
    2. Run everything 1-step-at-a-time
    
To run the everything all at once, all you have to do is to run the etl.py script via terminal. This will pull the all of the data within the data folder and parse the data to be inserted into the tables that has been created earlier using create_tables.py. If you'd like to understand the process of how the ETL work, you may do the 1-step-at-a-time by using the etl.ipynb or the etl version via Python Notebook. Additionally you can also check etl.py docstrings to understand the necessary steps that I've used to finish this ETL.

Thanks for checking it out!
Dan Agustinus
