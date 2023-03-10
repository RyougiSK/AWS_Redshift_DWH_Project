import time
import configparser
import boto3


def create_boto3_client(KEY, SECRET):
    """
    This function creates AWS clients for S3, IAM, and Redshift for later IAC (Infrastructure as Code) operations.

    :param KEY: AWS Access Key
    :param SECRET: AWS Secret Access Key
    :return: A tuple of boto3 clients for S3, IAM, and Redshift respectively.
    """

    # Create S3 resource
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


def delete_redshift_cluster(redshift, config):
    """
    This function deletes the Redshift cluster.

    :param redshift: The boto3 Redshift client
    :param config: The configparser object containing the AWS and Redshift configuration
    """
    try:
        # Delete the Redshift cluster with the specified identifier, skipping the final snapshot
        redshift.delete_cluster(
            ClusterIdentifier=config.get('DWH', 'DWH_CLUSTER_IDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )
        print('Cluster deleted')
    except Exception as e:
        print(e)


def main():
    """
    This function deletes the Redshift cluster.
    """

    # Read configuration from file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    # Get AWS access keys
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    # Create boto3 clients
    s3, iam, redshift = create_boto3_client(KEY, SECRET)

    # Delete the Redshift cluster
    delete_redshift_cluster(redshift, config)


if __name__ == '__main__':
    main()