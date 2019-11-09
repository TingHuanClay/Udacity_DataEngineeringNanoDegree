import pandas as pd
import boto3
import json
import psycopg2
import configparser
from botocore.exceptions import ClientError
import time

# Define the configuration parameters as global to be reused in each function
KEY                    = None
SECRET                 = None

DWH_CLUSTER_TYPE       = None
DWH_NUM_NODES          = None
DWH_NODE_TYPE          = None

DWH_CLUSTER_IDENTIFIER = None
DWH_DB                 = None
DWH_DB_USER            = None
DWH_DB_PASSWORD        = None
DWH_PORT               = None

DWH_IAM_ROLE_NAME      = None
    
def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Description: 
        Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

    Arguments:
        DWH_IAM_ROLE_NAME: specified role name

    Returns:
        roleArn: rolArn of specified role name
    """
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path = '/',
            RoleName = DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift Clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument = json.dumps(
                {
                    'Statement':[{'Action':'sts:AssumeRole',
                                  'Effect':'Allow',
                                  'Principal':{'Service': 'redshift.amazonaws.com'}}],
                    'Version':'2012-10-17'
                }
            )
        )
        
        # Attach Policy
        print('1.2 Attaching Policy')
        iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
                               PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
        # Get and print the IAM role ARN
        print('1.3 Get the IAM role ARN')
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(roleArn)
    except Exception as e:
        print(e)
    return roleArn

def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    """
    Description:
        Create a Redshift Cluster

    Arguments:
        DWH config values from the configuration file 

    Returns:
        None
    """

    try:
        response = redshift.create_cluster(        
            # add parameters for hardware
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),

            # add parameters for identifiers & credentials
            DBName = DWH_DB,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,

            # add parameter for role (to allow s3 access)
            IamRoles = [roleArn]
        )
        print('Redshift cluster is creating')
    except Exception as e:
        print(e)

def get_redshift_cluster_status(redshift):
    """
    Description:
        Retrieve Redshift clusters status

    Arguments:
        redshift

    Returns:
        cluster is 'available' or not
    """
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus']
    print("Cluster Status: {}".format(cluster_status))
    return cluster_status.lower()
    
def wait_until_cluster_available(redshift):
    """
    Description:
        Pooling check clusters status and wait 30 seconds until status is 'available'

    Arguments:
        redshift
    """
    t = 30
    while (get_redshift_cluster_status(redshift) != 'available'):
        print("wait for {} seconds and automaticaly check again".format(t))
        time.sleep(t)
        
def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Description:
        Retrieve Redshift clusters properties

    Arguments:
        redshift:
        DWH_CLUSTER_IDENTIFIER:

    Returns:
        myClusterProps: redshiftcluster properties
        DWH_ENDPOINT
        DWH_ROLE_ARN
    """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    # Print cluster properties for check
    print(prettyRedshiftProps(myClusterProps))

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    return myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN

def open_tcp_port(ec2, myClusterProps, DWH_PORT):
    """
    Description:
        Open an incoming TCP port to access the cluster ednpoint

    Arguments:
        ec2:
        myClusterProps
        DWH_PORT:

    Returns:
        myClusterProps: redshiftcluster properties
        DWH_ENDPOINT
        DWH_ROLE_ARN
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName = defaultSg.group_name,
            CidrIp = '0.0.0.0/0',
            IpProtocol = 'TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def main():
    global KEY, SECRET, \
        DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, \
        DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, \
        DWH_IAM_ROLE_NAME

    # Load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param":
                      ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                  "Value":
                      [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                 })
    
    # Create clients for EC2, S3, IAM, and Redshift
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

    iam = boto3.client('iam',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                      )

    redshift = boto3.client('redshift',
                                region_name="us-west-2",
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET
                           )
        
    
    # Step 1: Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
    print(roleArn)

    # Step 2: Create a Redshift Cluster
    print('2 Creating a Redshift Cluster')
    create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)
    
    wait_until_cluster_available(redshift)
    print("Redshift Cluster is Available now.")

    # Step 2-1: retrive the cluster properties we just created
    myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN= get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)

    # Step 3: Open an incoming TCP port to access the cluster ednpoint
    open_tcp_port(ec2, myClusterProps, DWH_PORT)

    # Step 4: Make sure you can connect to the cluster and close the connection in the end
    print('Creating Redshift Cluster connection...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Redshift Cluster is Connected')
    
    conn.close()
    print('Deleted Redshift Cluster connection')


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])    

if __name__ == "__main__":
    main()