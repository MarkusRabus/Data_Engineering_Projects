'''

Infrastructure as Code: script to check status of the Redshift cluster on AWS. The script also
writes the redshift cluster endpoint and IAM role ARN that gives access to Redshift to database 
in configuration file.

'''

import os
import pandas as pd
import boto3
from botocore.exceptions import ClientError

import configparser
config = configparser.ConfigParser()


def prettyRedshiftProps(props):
    """
    - Creates a panda dataframe which has important redshift parameters stored.
    - Returns pandas data frame with Redshift parameters as key
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])



####
# Read configuration file and assign variables
####

config.read_file(open( os.path.expanduser('~/dwh.cfg') ))
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")



####
# Create client for Redshift
####

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )


####
# Print out cluster parameters
####

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(prettyRedshiftProps(myClusterProps))


####
# Write the redshift cluster endpoint and IAM role ARN hat gives access to Redshift to database
# in the configuration file.
####

if myClusterProps['ClusterStatus'] == 'available':
	config.set("IAM_ROLE","ARN",myClusterProps['IamRoles'][0]['IamRoleArn'])
	config.set("CLUSTER","HOST",myClusterProps['Endpoint']['Address'])

config.write(open( os.path.expanduser('~/dwh.cfg'), 'w' ))
