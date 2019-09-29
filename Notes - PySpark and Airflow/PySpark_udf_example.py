## Author: Dan Agustinus Pesiwarissa
## Usage:
##     - The usage for this is to parse weird dates such as '01/Feb/2013:23:24:55 -0800'
##       into an acceptable format where PySpark DF can use it as a TimestampType
##     - By using the UDF, you will be able to apply a certain change by using your own way
##       such as using it to calculate or even do a certain regex and create new column and
##       field with the data that you've used in other columns


# Importing all the things that is needed
# At the moment, we're expecting that we already have the PySpark up and running
from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta
from time import strptime
from dateutil.parser import parse
import re

# Declare that this is using UDF and 'Timestamp' type is the result of it
@udf('Timestamp')
def utc_time_stamp(time_value):
    
    # This is for the default value in case there's a None or empty string
    if time_value == '' or time_value is None:
        time_value = '01/Jan/1970:01:00:00 -0100'
    
    # We use regex to find the necessary information from the given data
    day = int(re.search('^\d{2}', time_value).group(0))
    month = strptime(re.search('[A-z]{3}', time_value).group(0),'%b').tm_mon
    year = int(re.search('\d{4}', time_value).group(0))
    timezone = re.search('[+-]\d{4}$', time_value).group(0)
    
    # Finding all of the hours, mins, secs and putting them in each of their objects
    times_p = list(int(num.replace(':','')) for num in (re.findall(':\d{2}', time_value)))

    hour = times_p[0]
    mins = times_p[1]
    secs = times_p[2]
    
    # Create the datetime information from the datetimestamp
    datetime_object = datetime.strptime(time_value[:-6], '%d/%b/%Y:%H:%M:%S')
    
    # The -0800 is actually the time zone and the if/else below is to calculate the amount
    # of total minutes. I.E.: -0800 would be equal to -480 minutes time difference
    if '-' in timezone:
        min_diff = (abs(int(timezone[:3]) * 60) + int(timezone[3:])) * -1
    else:
        min_diff = (int(timezone[:3]) * 60) + int(timezone[3:])
    
    # using the timedelta, we're now "adding" the datetime with the timezone difference in minutes
    utc_date_time = datetime_object + timedelta(minutes=min_diff)
    
    return utc_date_time


# withColumn function is used to create a column within a PySpark df
clean_df = df.withColumn('utc_time', utc_time_stamp(df.date_time))

clean_df.printSchema()
#### Output from clean_df.printSchema()
# root
# |-- host: string (nullable = true)
# |-- client_identd: string (nullable = true)
# |-- user_id: string (nullable = true)
# |-- date_time: string (nullable = true)              <<< Old data
# |-- utc_time: timestamp (nullable = true)            <<< The new TIMESTAMP column
