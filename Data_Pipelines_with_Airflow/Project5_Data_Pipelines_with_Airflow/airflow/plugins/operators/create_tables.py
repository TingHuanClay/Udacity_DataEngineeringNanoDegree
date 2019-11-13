from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):
    ui_color = '#2a6733'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql = sql

    def execute(self, context):
        """
        Description:
            CreateTablesOperator is used for creating tables in redshift cluster
            according to the sql transfered from init
            Which can be traced to airflow/plugins/helpers/create_table_statements.py
        """
        self.log.info("Creating Redshift tables [START]")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Current Command:\n{}".format(self.sql))
        redshift_hook.run(self.sql)
        self.log.info("Creating Redshift tables [Finished]")