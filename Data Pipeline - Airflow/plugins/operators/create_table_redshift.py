from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class CreateTableRedshiftOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_config="",
                 *args, **kwargs):

        super(CreateTableRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_config = table_config
    
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        logging.info('Starting the table creation')
        data = {}
        create_table_sql = []
        
        with open(self.table_config,'r') as f:
            lines = list(line.strip().split('|') for line in f.readlines())

        for line in lines:
            if line[0] not in data.keys():
                data[line[0]] = {}
            data[line[0]][line[1]] = {'type':line[2], 'fields':line[3:]}

        for table in data:
            redshift_hook.run('DROP TABLE IF EXISTS {tbl}'.format(tbl=table))
            
            header = """
                        CREATE TABLE IF NOT EXISTS {tbl} (
                     """.format(tbl=table)
            fields = ''
            for col in data[table]:
                fields += '\t' + col + '\t' + data[table][col]['type'] + '\t' + ' '.join(data[table][col]['fields']) + ',' + '\n'

                sql_statement = header + '\n' + fields[:-2] + '\n)'

            create_table_sql.append(sql_statement)
        

        for table in create_table_sql:
            logging.info('Creating tables using the queries')
            try:
                redshift_hook.run(table)
            except:
                logging.info('Failed to create table using the query below:\n' + table)
        