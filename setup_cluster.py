import time
import boto3
import json
import configparser
import time
from botocore.exceptions import ClientError


def create_boto3_client(KEY, SECRET):
    '''
    Create AWS clients for S3, IAM, and Redshift for later Infrastructure as Code (IAC) operations.

    Args:
    - KEY (str): AWS access key ID
    - SECRET (str): AWS secret access key

    Returns:
    - s3 (boto3.resource): S3 client
    - iam (boto3.client): IAM client
    - redshift (boto3.client): Redshift client
    '''

    # Create S3 client
    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    # Create IAM client
    iam = boto3.client('iam', aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                       )

    # Create Redshift client
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    return s3, iam, redshift


def create_redshifts3_role(config, iam):
    '''
    Create a role that can read files from S3.

    Args:
    - config (ConfigParser): Configuration object
    - iam (boto3.client): IAM client

    Returns:
    - iam_role (str): ARN of the IAM role created
    '''

    # Get the name of the IAM role from the configuration object
    DWH_IAM_ROLE_NAME = config.get('IAM_ROLE', 'DWH_IAM_ROLE_NAME')

    try:
        print("Creating a new IAM Role")

        # Create a new IAM role with the specified name and permissions
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}]})
        )
    except Exception as e:
        print("Role is already exist")

    # Attach a policy that allows the IAM role to read files from S3
    print("Start Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    # Get the ARN of the IAM role created
    print("Get the IAM role ARN")
    iam_role = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print("ARN is:", iam_role)

    return iam_role


def create_redshift_cluster(redshift, config, iam_role):
    """
    Creates a new Redshift cluster with the given configuration and IAM role.

    Args:
        redshift: A boto3 Redshift client instance.
        config: A configparser ConfigParser instance with the configuration values.
        iam_role: The ARN of the IAM role used to access S3 data.

    Returns:
        None

    Raises:
        Exception: If the cluster creation fails, or if the cluster status cannot be retrieved.

    """
    # Get the configuration values from the config file
    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    try:
        # Create the Redshift cluster
        response = redshift.create_cluster(
            # Hardware configuration
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers and credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for S3 access)
            IamRoles=[iam_role]
        )
    except Exception as e:
        # Print the error message if the cluster creation fails
        print(e)

    # Wait until the cluster is completely active
    while True:
        try:
            # Get the cluster status
            cluster_status = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)["Clusters"][0]
        except Exception as e:
            # Print the error message if the cluster status cannot be retrieved
            print(e)
        if cluster_status["ClusterStatus"] == "available":
            # Break the loop if the cluster is available
            break
        # Print the current cluster status and wait for 10 seconds before checking again
        print("Cluster Status:", cluster_status["ClusterStatus"])
        print("----->")
        time.sleep(10)

    # Print the endpoint of the cluster
    print("Cluster is active")
    DWH_ENDPOINT = cluster_status["Endpoint"]["Address"]
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)


def main():
    """
    Set up the IAM role and Redshift cluster using the configuration values from the config file.

    Args:
        None

    Returns:
        None

    """
    # Read the configuration values from the config file
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    # Get the AWS credentials from the config file
    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")

    # Create the boto3 clients
    s3, iam, redshift = create_boto3_client(KEY, SECRET)

    # Create the IAM role for Redshift
    iam_role = create_redshifts3_role(config, iam)

    # Create the Redshift cluster
    create_redshift_cluster(redshift, config, iam_role)


if __name__ == "__main__":
    main()
