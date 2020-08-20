from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
        redshiftConnId = "",
        tableName = "",
        sqlStatement = "",
        appendData = True,
        awsCredentialsId = "",
        *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId = redshiftConnId
        self.tableName = tableName
        self.sqlStatement = sqlStatement
        self.appendData = appendData
        self.awsCredentialsId = awsCredentialsId

    def execute(self, context):
        self.log.info( ' --- Populating dim table {}'.format(self.tableName) )
        redshift = PostgresHook(postgres_conn_id=self.redshiftConnId)
        if self.appendData == True:
            self.log.info('Appending data to table {}'.format(self.tableName))
            insertStatement = "INSERT INTO {} {}".format(self.tableName, self.sqlStatement)
            redshift.run(insertStatement)
        else:
            self.log.info('Appending data')
            deleteStatement = "DELETE FROM {}".format(self.tableName)
            redshift.run(deleteStatement)
            insertStatement = "INSERT INTO {} {}".format(self.tableName, self.sqlStatement)
            redshift.run(insertStatement)

            
