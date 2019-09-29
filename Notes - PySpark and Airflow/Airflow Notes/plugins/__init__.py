"""
	This __init__ file help Airflow to 
	list out all of the operators and make sure
	that they are all available
"""

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.HasRowsOperator,
        operators.S3ToRedshiftOperator
    ]
