from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
   
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables = [],
                 conn_id = "redshift",
                 check_sql="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables
        self.check_sql = check_sql
        self.expected_result =expected_result

    def execute(self, context):
        redshift_hook = PostgresHook (self.conn_id)
        self.log.info("Running tests")
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality check failed. \
                {records[0][0]} does not equal {self.expected_result}
            """)
        else:
            self.log.info("Data quality check passed")