from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#EDF464'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 aws_creds='',
                 s3_bucket='',
                 s3_key='',
                 json_format='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.aws_creds=aws_creds
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_format=json_format

    
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_creds)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Starting staging of {}'.format(self.table))
        
        copy_sql = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    FORMAT AS JSON '{}'
                   """
        
        self.log.info("Starting Full-Incremental data from destination Redshift table")
        try:
            redshift.run("DELETE FROM {}".format(self.table))
        except:
            self.log.info("Bypassing the error from trying to delete from {}".format(self.table))
            
        self.log.info("Copying data from S3 to Redshift")

        s3_path = "s3://{bucket}/{key}".format(bucket=self.s3_bucket, key=self.s3_key)
        formatted_sql = copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
        )
        redshift.run(formatted_sql)