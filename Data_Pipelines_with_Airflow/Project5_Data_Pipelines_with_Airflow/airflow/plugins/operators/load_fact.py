from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 select_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        """
        Description:
            LoadFactOperator is used processing the fact table: songplays
            according to the sql transfered from init
            the definition of sql can be traced to airflow/plugins/helpers/sql_queries.py
        """
        self.log.info("Loading Fact Table ({}) [START]".format(self.table))
        self.log.info("SQL Command:\n{}".format(self.select_query))

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("GET redshift_hook")
#         redshift_hook.run(str(self.sql_query))
        redshift_hook.run(self.select_query)
        self.log.info("Loading Fact Table ({}) [FINISHED]".format(self.table))
