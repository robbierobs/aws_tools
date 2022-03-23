import sys
import yaml
import boto3
import datetime
from os import path
import math
import itertools


def choice_check(x, settings):
    funct_dict = {
        'm': menu,
        'menu': menu,
        'q': quit,
        'quit': quit,
    }

    if x in ('y', 'yes'):
        True
    elif x in funct_dict:
        if x in ('m', 'menu'):
            funct_dict[x](settings)
        funct_dict[x]()
    else:
        return False


def batch(item_list, batch_size, snapshots=False):
    batch_size = int(batch_size)
    print(f'''
          Starting objects: %s\t Batch Size: %s
          ''' % (len(item_list), batch_size))

    popped_list = []

    for x in range(0, batch_size, 1):
        item = item_list.pop()
        popped_list.append(item)

    batch = resource_generator(popped_list, batch_size, snapshots=snapshots)

    return (item_list, batch)


def batch_pre(start_date, settings):

    process = ['yes', 'y', 'q', 'quit', 'm', 'menu']

    confirm_delete = -1
    while confirm_delete not in process:
        confirm_delete = input('\nConfirm: Delete these items (y/m/q): ')
        confirm_delete = confirm_delete.lower()
        choice_check(confirm_delete, settings)

    if confirm_delete:
        print(f'\nDeleting items older than %s.' % (start_date))

        batch_confirm = -1
        while batch_confirm not in process:
            batch_confirm = input('\nDelete in batches? (y/m/q): ')
            batch_confirm = batch_confirm.lower()
            choice_check(batch_confirm, settings)
        if batch_confirm:
            while True:
                batch_size = input('\nInput the desired batch size to delete at once: ')
                try:
                    batch_size = int(batch_size)
                    break
                except:
                    continue
            return batch_size


def snapshot_validation(item_list, days):

    start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(int(days))
    start_date = start_date.date()

    backup = 'awsbackup:'

    refined_list = []

    for item in item_list:
        if backup in item['DBClusterSnapshotIdentifier']:
            print(item)
            continue
        else:
            creation_date = item['SnapshotCreateTime']
            creation_date = creation_date.date()
            if creation_date < start_date:
                refined_list.append(item)

    return refined_list


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
    return client


def cleaning_selection(choice, settings, db=False):

    backup_functions = {
        'a': backup_cleaner,
        'b': rds_cleaner
    }

    db_functions = {
        'a': snapshot_cleaner,
        'b': describe_db_instance_automated_backups,
        'c': describe_db_snapshots
    }

    if db:
        if choice in db_functions:
            db_functions[choice](settings)
        else:
            print('Selection not available')
            menu(settings)
    else:
        if choice in backup_functions:
            backup_functions[choice](settings)
        else:
            print('Selection not available')
            menu(settings)


def menu(settings, db=False):

    main_menu_selection = ['a', 'b']
    db_menu_selection = ['a', 'b', 'c']

    menu_text = """

    A.) AWS Backup Cleaning
    B.) RDS Cleaning
    """

    db_text = """
    A.) Cluster Snapshots
    B.) Automated Instance Backups
    C.) DB Instance Snapshot
    """

    if db:
        print(db_text)
    else:
        print(menu_text)

    menu_selection = -1
    if db:
        while menu_selection not in db_menu_selection:
            menu_selection = input('Select: ')
            menu_selection = menu_selection.lower()
    else:
        while menu_selection not in main_menu_selection:
            menu_selection = input('Select: ')
            menu_selection = menu_selection.lower()

    cleaning_selection(menu_selection, settings, db)


def snapshot_cleaner(settings):


    created_before = input('Select snapshots that were created how many days ago? ')
    start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(int(created_before))

    snapshot_list = describe_db_cluster_snapshots(settings)
    # Checking each snapshot to for the correct date and to be sure it
    # is not an AWS Backup resource
    snapshot_list = snapshot_validation(snapshot_list, created_before)
    top_5 = resource_generator(snapshot_list, 5, snapshots=True)
    # snap_list = resource_generator(snapshot_list, snapshots=True)
    for item in top_5:
        print(preview_string(item, snapshots=True))

    print(f'\nFound %s snapshot(s).\n' % (len(snapshot_list)))

    batch_size = batch_pre(start_date, settings)

    batch_delete(
        items=snapshot_list,
        batch_size=batch_size,
        snapshots=True,
        backup=False,
        settings=settings)

def describe_db_cluster_snapshots(settings):
    client = get_client(settings, db=True)
    response = client.describe_db_cluster_snapshots(
        # DBInstanceIdentifier='string',
        # DBSnapshotIdentifier='string',
        # SnapshotType='string',
        # Filters=[
        #     {
        #         'Name': 'string',
        #         'Values': [
        #             'string',
        #         ]
        #     },
        # ],
        MaxRecords=100,
        # Marker='string',
        IncludeShared=True,
        IncludePublic=True,
        # DbiResourceId='string'
    )
    snapshot_list = []
    for snapshot in response['DBClusterSnapshots']:
        snapshot_list.append(snapshot)

    return snapshot_list


def describe_db_snapshots(settings):
    client = get_client(settings, db=True)
    response = client.describe_db_snapshots(
        # DBInstanceIdentifier='string',
        # DBSnapshotIdentifier='string',
        # SnapshotType='string',
        # Filters=[
        #     {
        #         'Name': 'string',
        #         'Values': [
        #             'string',
        #         ]
        #     },
        # ],
        MaxRecords=100,
        # Marker='string',
        IncludeShared=True,
        IncludePublic=True,
        # DbiResourceId='string'
    )
    print(response)
    snapshot_list = []
    for snapshot in response['DBSnapshots']:
        snapshot_list.append(snapshot)

    return snapshot_list

def describe_db_clusters(settings):
    client = get_client(settings,db=True)
    response = client.describe_db_clusters(
        # DBClusterIdentifier=db_cluster,
        # Filters=[
        #     {
        #         'Name': 'string',
        #         'Values': [
        #             'string',
        #         ]
        #     },
        # ],
        MaxRecords=100,
        # Marker='string',
        IncludeShared=True
    )
    cluster_list = []
    for cluster in response['DBClusters']:
        cluster_list.append(cluster)

    return cluster_list


def describe_db_instance_automated_backups(client):
    response = client.describe_db_instance_automated_backups(
        # DbiResourceId='string',
        # DBInstanceIdentifier='string',
        # Filters=[
        #     {
        #         'Name': 'string',
        #         'Values': [
        #             'string',
        #         ]
        #     },
        # ],
        MaxRecords=100,
        # Marker='string',
        # DBInstanceAutomatedBackupsArn='string'
    )
    return response

def resource_generator(items, items_to_show=None, db_cluster=False, snapshots=False):

    resource_list = []
    attributes_dict = {
        'db_cluster': [
            ('DBClusterIdentifier', 'cluster_identifier'),
            ('DBClusterMembers', 'cluster_members'),
            ('DBClusterArn', 'cluster_arn')
        ],
        'backups': [
            ('RecoveryPointArn', 'arn'),
            ('ResourceType', 'resource_type'),
            ('CreationDate', 'created'),
            ('BackupSizeInBytes', 'size')
        ],
        'snapshots': [
            ('DBClusterIdentifier', 'db_cluster'),
            ('DBClusterSnapshotArn', 'arn'),
            ('SnapshotCreateTime', 'created'),
            ('AllocatedStorage', 'size'),
            ('DBClusterSnapshotIdentifier', 'name')

        ]
    }

    db_cluster = (f'{db_cluster=}'.split('=')[0], db_cluster)
    snapshots = (f'{snapshots=}'.split('=')[0], snapshots)

    options = [
        db_cluster,
        snapshots
    ]

    for option in options:
        if option[1]:
            attributes = attributes_dict.get(option[0])
            break
        else:
            attributes = attributes_dict.get('backups')

    if items_to_show:
        items = itertools.islice(items, items_to_show)

    for resource in items:
        item = {}
        for key in attributes:
            if key[0] in resource:
                if key[0] == 'CreationDate':
                    resource[key[0]] = str(resource[key[0]])
                elif key[0] == 'DBClusterMembers':
                    members = []
                    for member in resource[key[0]]:
                        members.append(member)
                    # item[key[1]] =
                    item[key[1]] = members
                    continue
                item[key[1]] = resource[key[0]]
        resource_list.append(item)

    return resource_list


def delete_recovery_point(settings, vault_name, backup_arn):
    client = get_client(settings, backup=True)
    response = client.delete_recovery_point(
        BackupVaultName=vault_name,
        RecoveryPointArn=backup_arn
    )
    return response


def delete_db_cluster_snapshot(settings, name):
    client = get_client(settings, db=True)
    response = client.delete_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=name
    )
    return response


def batch_delete(items, batch_size, settings, vault_name=None, backup=None, db=None, snapshots=False):

    confirm_delete = -1
    bail = ['quit', 'q', 'm', 'menu']

    if len(items) - batch_size < 0:
        difference = len(items) - batch_size
        batch_size = batch_size + difference
        print(f'\nThe batch size has been adjusted from %s to %s as there (is/are) only %s item(s) to delete.' % (
            (batch_size - difference), batch_size, len(items)))

    while (len(items) - batch_size >= 0) and (confirm_delete not in bail):
        batch_list = batch(items, batch_size, snapshots=snapshots)
        items = batch_list[0]
        items_to_delete = batch_list[1]

        for item in items_to_delete:
            print(preview_string(item, snapshots=snapshots))

        confirm_delete = -1
        while confirm_delete not in ('yes', 'y', 'c', 'm', 'q', 'quit', 'change', 'menu'):
            confirm_delete = input('\nConfirm: Delete these items (y/c/m/quit): ')
            confirm_delete = confirm_delete.lower()

        choice_check(confirm_delete, settings)

        if confirm_delete in ('y', 'yes'):
            for x in range(0, batch_size, 1):
                if vault_name and backup:
                    item = items_to_delete.pop()
                    arn = item['arn']
                    # client = get_client(settings, backup=True)
                    response = delete_recovery_point(settings, vault_name, arn)
                    status_code = response['ResponseMetadata']['HTTPStatusCode']
                    print(f'%s \n\t Status Code: %s' % (preview_string(item), status_code))
                if snapshots:
                    item = items_to_delete.pop()
                    cluster = item['db_cluster']
                    name = item['name']
                    response = delete_db_cluster_snapshot(settings, name)
                    status_code = response['ResponseMetadata']['HTTPStatusCode']
                    print(f'%s \n\t Status Code: %s' % (preview_string(item), status_code))
            print(f'''
                  \n\tBatch deletion complete for %s items.
                  Ending objects: %s
                  ''' % (batch_size, str(len(items))))

        if confirm_delete in ('c', 'change'):
            batch_size = input('\nChange batch size: ')
            batch_size = int(batch_size)


def preview_string(item, db_cluster=False, snapshots=False):

    attributes_dict = {
        'db_cluster': [
            ('cluster_identifier', 'Cluster Identifier'),
            ('cluster_members', 'Number of Members'),
            ('cluster_arn', 'Arn')
        ],
        'backups': [
            ('resource_type', 'Resource Type'),
            ('created', 'Created'),
            ('size', 'Size'),
            ('arn', 'Arn')
        ],
        'snapshots': [
            ('name', 'Name'),
            ('db_cluster', 'DB Cluster'),
            ('arn', 'Arn'),
            ('created', 'Created'),
            ('size', 'Size')
        ]

    }

    db_cluster = (f'{db_cluster=}'.split('=')[0], db_cluster)
    snapshots = (f'{snapshots=}'.split('=')[0], snapshots)

    options = [
        db_cluster,
        snapshots
    ]
    for option in options:
        if option[1]:
            attributes = attributes_dict.get(option[0])
            break
        else:
            attributes = attributes_dict.get('backups')


    key_values = []
    for key in attributes:
        if key[0] in item:
            if key[0] == 'cluster_members':
                item[key[0]] = len(item[key[0]])
            key_values.append(key[1] + ': ' + str(item[key[0]]))


    preview = '\t'.join(key_values)
    return preview


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def parse_yaml(env_file):
    with open(env_file) as file:
        settings = yaml.full_load(file)
    return settings


def backup_cleaner(settings):

    client = get_client(settings=settings, backup=True, db=False)

    backup_vault_list = client.list_backup_vaults(
        # NextToken='string',
        MaxResults=123
    )

    backup_vault_list = backup_vault_list['BackupVaultList']
    for idx, val in enumerate(backup_vault_list):
        vault_name = val['BackupVaultName']
        vault_arn = val['BackupVaultArn']

        print(f'''
              [%s]:\tVault Name:\t%s\tVault Arn:\t%s
              ''' % (idx, vault_name, vault_arn))


    vault_selection = input('Select backup vault by index: ')

    vault = backup_vault_list[int(vault_selection)]

    vault_arn = vault['BackupVaultArn']
    vault_name = vault['BackupVaultName']

    backup_plans = client.list_backup_plans(
        # NextToken='string',
        MaxResults=123,
        IncludeDeleted=True
    )

    backup_plan_list = backup_plans['BackupPlansList']
    for idx, val in enumerate(backup_plan_list):
        backup_name = val['BackupPlanName']
        backup_id = val['BackupPlanId']

        print(f'''
              [%s]:\tBackup Name:\t%s\tBackup ID:\t%s
              ''' % (idx, backup_name, backup_id))

    backup_selection = input('Select backup plan by index: ')

    backup = backup_plan_list[int(backup_selection)]

    backup_name = backup['BackupPlanName']
    backup_id = backup['BackupPlanId']

    created_before = input('Select recovery points that were created how many days ago? ')
    start_date = datetime.datetime.now() - datetime.timedelta(int(created_before))


    recovery_points = client.list_recovery_points_by_backup_vault(
        BackupVaultName=vault_name,
        # MaxResults=500,
        ByBackupPlanId=backup_id,
        ByCreatedBefore=start_date,
    )

    recovery_points = recovery_points['RecoveryPoints']
    total_objects = len(recovery_points)

    top_5 = resource_generator(recovery_points, 5)

    preview_list = ['\n\n']
    for item in top_5:
        preview_list.append(preview_string(item))
    print('\n'.join(preview_list))

    print(f'''
          \n!!!!! CAUTION: THIS IS A DESTRUCTIVE ACT AND CANNOT BE UNDONE !!!!
          \n
          Backup Vault: %s
          Backup Plan: %s
          Total Objects: %s
          Created before: %s (%s days and older)
         \n
          ''' %
          (vault_name, backup_id, total_objects, start_date, created_before)
         )

    batch_size = batch_pre(start_date, settings)

    batch_delete(
        items=recovery_points,
        vault_name=vault_name,
        batch_size=batch_size,
        backup=True,
        settings=settings)


def rds_cleaner(settings):

    client = get_client(settings, db=True)

    cluster_list = describe_db_clusters(settings)
    clusters = resource_generator(items=cluster_list, db_cluster=True)

    print(f'\nFound %s cluster(s).\n' % (len(clusters)))
    print(preview_string(clusters[0], db_cluster=True))

    snapshots = describe_db_cluster_snapshots(settings)
    print(f'\nFound %s snapshots. (Max of 100 found at a time).' % (len(snapshots)))

    automated_backups = describe_db_instance_automated_backups(client)
    print(f'Found %s automated backups. (Max of 100 found at a time).' % (len(automated_backups)))

    menu(settings, db=True)


if __name__ == "__main__":

    arg = sys.argv[1:]
    if len(arg) < 1:
        print('Please provide a deployment environment.')
        quit()
    env_file = str(arg[0]) + '.yml'

    if not path.exists(env_file):
        print('Please create a yaml file. ex. ' + env_file)
        quit()
    settings = parse_yaml(env_file)

    menu(settings)
