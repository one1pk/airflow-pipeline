from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql_checks = sql_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.sql_checks:
            sql = check.get('sql')
            expected_result = check.get('expected_result')

            records = redshift_hook.get_records(sql)[0]

            if records[0] != expected_result:
                raise ValueError(f"Data quality check failed. Expected result not matched")