from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_creds='',
                 table='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_creds=aws_creds
        self.table=table

    def execute(self, context):
        self.log.info('Starting Data Quality check and see if the columns are filled')
        aws_hook = AwsHook(self.aws_creds)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshift.get_records("SELECT COUNT(*) FROM {table}".format(table=self.table))
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. {table} returned no results".format(table=self.table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. {table} contained 0 rows".format(table=self.table))
        logging.info("Data quality on table {table} check passed with {rec} records".format(table=self.table, rec=records[0][0]))