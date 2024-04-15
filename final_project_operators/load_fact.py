from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert_sql = "INSERT INTO {}  {}"
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table=table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql
        )

        redshift_hook.run(formatted_sql)
