from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    insert_sql = "INSERT INTO {}  {}"
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 truncate = False,
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.truncate = truncate
        self.table=table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if(self.truncate):
            redshift_hook.run("TRUNCATE " + self.table)

        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql
        )

        redshift_hook.run(formatted_sql)
