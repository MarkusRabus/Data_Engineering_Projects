'''

Infrastructure as Code: script to delete the Redshift cluster on AWS

'''

import os
import boto3
from botocore.exceptions import ClientError

import configparser
config = configparser.ConfigParser()



####
# Read configuration file and assign variables
####

config.read_file(open( os.path.expanduser('~/dwh.cfg') ))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("DWH","DWH_IAM_ROLE_NAME")


####
# Create clients for IAM and Redshift
####

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )


####
# Delete Redshift Cluster
####
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


####
# Detach policy and delete Role
####

iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

