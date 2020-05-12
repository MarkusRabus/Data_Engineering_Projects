
'''

Infrastructure as Code: script to create IAM role and create the Redshift cluster on AWS

'''

import os
import boto3
import json
from botocore.exceptions import ClientError
import configparser
config = configparser.ConfigParser()


####
# Read configuration file and assign variables
####

config.read_file(open( os.path.expanduser('~/dwh.cfg') ))
KEY                    	= config.get('AWS','KEY')
SECRET                 	= config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       	= config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          	= config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          	= config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER 	= config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      	= config.get("DWH", "DWH_IAM_ROLE_NAME")

DWH_DB                 	= config.get("CLUSTER","DB_NAME")
DWH_DB_USER            	= config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        	= config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               	= config.get("CLUSTER","DB_PORT")



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
# Create the role
####

try:
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
				'Effect': 'Allow',
				'Principal': {'Service': 'redshift.amazonaws.com'}}],
				'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)


####
# Attach policy to role
####

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']


####
# Create Redshift cluster
####

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn'] 	# Get the role ARN 
																	# that gives access to Redshift

try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)

