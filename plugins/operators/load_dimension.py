from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
   
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = "redshift",
                 table= "",
                 sql_query= "",
                 mode="append-only",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        self.log.info("Connecting to redshift")
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        if self.mode == "delete-load":
            self.log.info("Deleting data on Redshift")
            redshift.run("TRUNCATE {}".format(self.table))
        self.log.info("Runnig INSERT query to load data from S3 Bucket to Redshift")
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
