import argparse
import os
import configparser
import boto3
import json
import time

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#Read cluster properties/constants
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

#Read key/secret from env
KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

#Utility Function to create required AWS resources
def create_required_resources():
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    return ec2, s3, iam, redshift

# Creates IAM role and redshift cluster
def create_RS_cluster(ec2, s3, iam, redshift):
    """Creates IAM role and redshift cluster"""
    
    #1.1 Creating a new IAM Role
    try:
        print("1.1 Start - Creating a new IAM Role") 
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
        print("1.1 Done - Creating a new IAM Role")
    except Exception as e:
        print("1.1 ")
        print(e)
    
    #1.2 Attaching Policy
    print("1.2 Start - Attaching policy to IAM role")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print("1.2 Done - Attaching policy to IAM role")
    
    #2.0 Create redshift cluster
    print("2.0 Start - Create reshift cluster")
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
        
        print("2.0 Done - Create reshift cluster")
    except Exception as e:
        print("2.0 ")
        print(e)
        
    #2.1 Check until cluster is available
    print("2.1 Start - Checking cluster for available status")
    timestep = 10
    for _ in range(int(600/timestep)):
        clusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if clusterProps['ClusterStatus'] == 'available':
            break
        print('Cluster status is "{}". Retrying in {} seconds.'.format(clusterProps['ClusterStatus'], timestep))
        time.sleep(timestep)
    
    print("2.1 Done - Checking cluster for available status")
    
    #3.0 Open an incoming TCP port to access the cluster endpoint
    print("3.0 Start - Opening incoming TCP port")
    try:
        vpc = ec2.Vpc(id=clusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        #print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
        print("3.0 Done - Opening incoming TCP port")
    except Exception as e:
        print("3.0 ")
        print(e)
        
# Deletes IAM role and redshift cluster
def delete_RS_cluster(iam, redshift):
    """Deletes IAM role and redshift cluster"""
    #1.0 Delete cluster
    print("1.0 Start - Delete Cluster")
    
    try:
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        clusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        print("Cluster status "+ clusterProps['ClusterStatus'])
    except Exception as e:
        print("1.0 ")
        print(e)
        
    print("1.0 Done - Delete Cluster")
    
    #2.0 Delete IAM role
    print("2.0 Start - Delete IAM Role")
    
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    
    print("2.0 Done - Delete IAM Role")
    

def get_Cluster_Props(redshift=None):
    """Return cluster endpoint & role arn to the caller"""
    if redshift is None:
        ec2, s3, iam, redshift = create_required_resources()
        
    clusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    dhw_endpoint = clusterProps['Endpoint']['Address']
    dwh_roleArn  = clusterProps['IamRoles'][0]['IamRoleArn']
    return dhw_endpoint, dwh_roleArn

# Main function
def main(args):
    # Create required resources
    ec2, s3, iam, redshift = create_required_resources()
    
    if args.print:
        #print(get_Cluster_Props(redshift))
        print(get_Cluster_Props())
    # For Delete
    elif args.delete:
        delete_RS_cluster(iam, redshift)
    # DEfault For Create 
    else:
        create_RS_cluster(ec2, s3, iam, redshift)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #Default delete argument to false
    parser.add_argument('--delete', dest='delete', default=False, action='store_true')
    parser.add_argument('--print', dest='print', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
        
