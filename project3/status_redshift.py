import os
import pandas as pd
import boto3
from botocore.exceptions import ClientError

import configparser
config = configparser.ConfigParser()
config.read_file(open( os.path.expanduser('~/dwh.cfg') ))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )


myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(prettyRedshiftProps(myClusterProps))


if myClusterProps['ClusterStatus'] == 'available':
	config.set("IAM_ROLE","ARN",myClusterProps['IamRoles'][0]['IamRoleArn'])
	config.set("CLUSTER","HOST",myClusterProps['Endpoint']['Address'])

config.write(open( os.path.expanduser('~/dwh.cfg'), 'w' ))
