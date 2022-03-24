import yaml
import boto3
import datetime

def parse_yaml(env_file):
    with open(env_file) as file:
        settings = yaml.full_load(file)
    return settings


def get_client(settings, db=False, backup=False):

    sts_client = boto3.client('sts')

    assumed_role_object = sts_client.assume_role(
        RoleArn=settings['role'],
        RoleSessionName='backup-plan-cleanup'
    )

    credentials = assumed_role_object['Credentials']

    if backup:
        client_scope = 'backup'
    elif db:
        client_scope = 'rds'


    client = boto3.client(
        client_scope,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    client._client_config._user_provided_options['tcp_keepalive'] = True
    client._client_config._user_provided_options['max_pool_connections'] = 25
    return client


def snapshot_validation(item_list, days):

    start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(int(days))
    start_date = start_date.date()

    backup = 'awsbackup:'

    refined_list = []

    for item in item_list:
        if backup in item['DBClusterSnapshotIdentifier']:
            continue
        else:
            creation_date = item['SnapshotCreateTime']
            creation_date = creation_date.date()
            if creation_date < start_date:
                refined_list.append(item)

    return refined_list

