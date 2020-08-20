from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    countNULL_sql = """
        SELECT COUNT(*) 
        FROM {} 
        WHERE {} is NULL;
    """
    
    @apply_defaults
    def __init__(self,
        redshiftConnId='redshift',
        tableName='',
        columnName='',
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId = redshiftConnId
        self.tableName = tableName
        self.columnName = columnName

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshiftConnId)
        self.log.info('Checking table {}, column {} for NULL elements!'.format(self.tableName, self.columnName))
        result = hook.get_records(DataQualityOperator.countNULL_sql.format(self.tableName, self.columnName))
        nullelements = result[0][0]
        self.log.info('{} NULL elements found in table {}, column {}'.format(nullelements,self.tableName, self.columnName))
        if nullelements != 0.:
            raise Exception('{} NULL elements found!'.format(nullelements))
        self.logger.info("Check passed!")
        

        