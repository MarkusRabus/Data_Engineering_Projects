import boto3
import os
from botocore.exceptions import ClientError

import configparser
config = configparser.ConfigParser()
config.read_file(open( os.path.expanduser('~/dwh.cfg') ))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )


sampleDbBucket =  s3.Bucket("s3://udacity-dend/song_data")
for obj in sampleDbBucket.objects.all():
     print(obj)
