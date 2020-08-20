from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
        redshiftConnId = "",
        tableName = "",
        sqlStatement = "",
        awsCredentialsId = "",
        *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId = redshiftConnId
        self.tableName = tableName
        self.sqlStatement = sqlStatement
        self.awsCredentialsId = awsCredentialsId

    def execute(self, context):
        self.log.info( ' --- Populating fact table {}'.format(self.tableName) )
        redshift = PostgresHook(postgres_conn_id=self.redshiftConnId)
        self.log.info('Appending data to table {}'.format(self.tableName))
        insertStatement = "INSERT INTO {} {}".format(self.tableName, self.sqlStatement)
        redshift.run(insertStatement)

        
