from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}' 
        compupdate off region 'us-west-2';
    """
    
    
    
    @apply_defaults
    def __init__(self,
                 redshiftConnId="",
                 tableName="",
                 awsCredentialsId="",
                 s3Bucket="",
                 s3Key="",
                 jsonPath = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId = redshiftConnId
        self.tableName = tableName
        self.s3Bucket = s3Bucket
        self.s3Key = s3Key
        self.awsCredentialsId = awsCredentialsId
        self.jsonPath = jsonPath
        
    def execute(self, context):
        awsHook = AwsHook(self.awsCredentialsId)
        credentials = awsHook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshiftConnId)
        self.log.info('--- Start staging {}'.format(self.tableName))
        renderedKey = self.s3Key.format(**context)
        s3Path = "s3://{}/{}".format(self.s3Bucket, renderedKey)
        formattedSQL = StageToRedshiftOperator.copy_sql.format(
            self.tableName,
            s3Path,
            credentials.access_key,
            credentials.secret_key,
            self.jsonPath
        )
        redshift.run(formattedSQL)
        self.log.info('Copy data from JSON to posgres {}'.format(self.tableName))




