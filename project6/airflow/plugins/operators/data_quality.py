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
        dataQualityChecks={'':'',},
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId = redshiftConnId
        self.dataQualityChecks = dataQualityChecks

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshiftConnId)
        self.log.info('--- Data quality checks!')
        errors = 0
        failed = []
        for check_nr,check in enumerate(self.dataQualityChecks):
            self.log.info('Data quality test {}'.format(check_nr))
            sqlString = check.get('checkSQL')
            expectedResult = check.get('expectedResult')
            result = hook.get_records(sqlString)[0]
            self.log.info('Result: {}, expected: {}'.format(result[0], expectedResult))
            if expectedResult != result[0]:
                errors += 1
                failed.append(sqlString)
        if errors > 0:
            self.log.info('{} checks failed'.format(errors))
            self.log.info(failed)
            raise ValueError('Data quality check failed')
        else:
            self.logger.info("Check passed!")
        

        