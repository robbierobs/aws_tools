import sys
import getopt
import datetime
import math
import itertools
import concurrent
import concurrent.futures
import time

from utils import parse_yaml, get_client, snapshot_validation

multiprocess = False

def try_multiprocess(i, vault_name, items_to_delete, snapshots, backup, settings, auto_mode, batch_size):
    item = items_to_delete[i]
    if auto_mode:
        wait_time = batch_size / 20
        time.sleep(wait_time)
    if vault_name and backup:
        arn = item['arn']
        response = delete_recovery_point(settings, vault_name, arn)
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        print(f'%s \n\t Status Code: %s' % (preview_string(item), status_code))
    if snapshots:
        name = item['name']
        response = delete_db_cluster_snapshot(settings, name)
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        print(f'%s \n\t Status Code: %s' % (preview_string(item), status_code))


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

    main_menu_selection = ['a', 'b', 'q', 'quit']
    db_menu_selection = ['a', 'b', 'c', 'q', 'quit']

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

    if menu_selection in ('q', 'quit'):
        quit()

    cleaning_selection(menu_selection, settings, db)


def snapshot_cleaner(settings):

    created_before = input('Select snapshots that were created how many days ago? ')
    start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(int(created_before))

    snapshot_list = describe_db_cluster_snapshots(settings)
    # Checking each snapshot to for the correct date and to be sure it
    # is not an AWS Backup resource
    snapshot_list = snapshot_validation(snapshot_list, created_before)
    total_objects = len(snapshot_list)

    top_5 = resource_generator(snapshot_list, 5, snapshots=True)

    for item in top_5:
        print(preview_string(item, snapshots=True))

    print(f'\nFound %s snapshot(s).\n' % (len(snapshot_list)))

    batch_size = batch_pre(start_date, settings)
    print(f'''
          \n!!!!! CAUTION: THIS IS A DESTRUCTIVE ACT AND CANNOT BE UNDONE !!!!
          \n
          Total Objects: %s
          Created before: %s (%s days and older)
          \n
          ''' %
          (total_objects, start_date, created_before))
    batch_delete(
        items=snapshot_list,
        batch_size=batch_size,
        snapshots=True,
        backup=False,
        settings=settings)


def list_recovery_points_by_backup_vault(settings, vault_name, backup_id, start_date):
    recovery_point_list = []
    token = ''

    print('\nCompiling recovery point list...', end='')
    while True:
        print('...', end='', flush=True)
        client = get_client(settings, backup=True, db=False)
        if token:
            response = client.list_recovery_points_by_backup_vault(
                BackupVaultName=vault_name,
                MaxResults=500,
                ByBackupPlanId=backup_id,
                ByCreatedBefore=start_date,
                NextToken=token
            )
        else:
            response = client.list_recovery_points_by_backup_vault(
                BackupVaultName=vault_name,
                MaxResults=500,
                ByBackupPlanId=backup_id,
                ByCreatedBefore=start_date
            )

        if 'NextToken' in response:
            token = response['NextToken']
        else:
            token = ''

        for point in response['RecoveryPoints']:
            recovery_point_list.append(point)

        if not token:
            print('\n')
            break

    return recovery_point_list

def describe_db_cluster_snapshots(settings):
    snapshot_list = []
    marker = ''

    print('\nCompiling snapshot list...', end='')
    while True:
        print('...', end='', flush=True)
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
            Marker=marker,
            IncludeShared=True,
            IncludePublic=True,
            # DbiResourceId='string'
        )
        if 'Marker' in response:
            marker = response['Marker']
        else:
            marker = ''

        for snapshot in response['DBClusterSnapshots']:
            snapshot_list.append(snapshot)

        if not marker:
            print('\n')
            break

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

    snapshot_list = []
    for snapshot in response['DBSnapshots']:
        snapshot_list.append(snapshot)

    print(snapshot_list)

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

    def batch_size_validation(items, batch_size):
        if len(items) - batch_size < 0:
            difference = len(items) - batch_size
            if -difference != len(items):
                batch_size = batch_size + difference
                print(f'\nThe batch size has been adjusted from %s to %s as there (is/are) only %s item(s) to delete.' % (
                    (batch_size - difference), batch_size, len(items)))
        return batch_size

    auto_mode = False
    confirm_delete = -1
    bail = ['quit', 'q', 'm', 'menu']

    batch_size = batch_size_validation(items, batch_size)

    # if len(items) - batch_size < 0:
    #     difference = len(items) - batch_size
    #     batch_size = batch_size + difference
    #     print(f'\nThe batch size has been adjusted from %s to %s as there (is/are) only %s item(s) to delete.' % (
    #         (batch_size - difference), batch_size, len(items)))

    while (len(items) - batch_size >= 0) and (confirm_delete not in bail):
        batch_size = batch_size_validation(items, batch_size)
        batch_list = batch(items, batch_size, snapshots=snapshots)
        items = batch_list[0]
        items_to_delete = batch_list[1]

        for item in items_to_delete:
            print(preview_string(item, snapshots=snapshots))

        if auto_mode:
            confirm_delete = 'y'

        confirm_delete = -1
        if auto_mode:
            confirm_delete = 'y'
        while confirm_delete not in ('yes', 'y', 'c', 'm', 'q', 'a', 'quit', 'change', 'menu', 'auto'):
            confirm_delete = input('\nConfirm: Delete these items (y/c/m/a/quit): ')
            confirm_delete = confirm_delete.lower()

        choice_check(confirm_delete, settings)

        if confirm_delete in ('y', 'yes', 'a', 'auto'):
            if confirm_delete in ('a', 'auto'):
                auto_mode = True

            global multiprocess
            if multiprocess:
                with concurrent.futures.ThreadPoolExecutor(16) as e:
                    fut = [e.submit(try_multiprocess, i, settings=settings, vault_name=vault_name, backup=backup, items_to_delete=items_to_delete, snapshots=snapshots, auto_mode=auto_mode, batch_size=batch_size) for i in range(0, batch_size, 1)]
                    for r in concurrent.futures.as_completed(fut):
                        r.result()
            else:
                for x in range(0, batch_size, 1):
                    if vault_name and backup:
                        item = items_to_delete.pop()
                        arn = item['arn']
                        response = delete_recovery_point(settings, vault_name, arn)
                        status_code = response['ResponseMetadata']['HTTPStatusCode']
                        print(f'%s \n\t Status Code: %s' % (preview_string(item), status_code))
                    if snapshots:
                        item = items_to_delete.pop()
                        name = item['name']
                        response = delete_db_cluster_snapshot(settings, name)
                        status_code = response['ResponseMetadata']['HTTPStatusCode']
                        print(f'%s \n\t Status Code: %s' % (preview_string(item), status_code))
            print(f'''
                  \n\tBatch deletion complete for %s items.
                  Ending objects: %s
                  ''' % (batch_size, str(len(items))))

        # if confirm_delete in ('a', 'auto'):
        #     auto_mode = True

        if confirm_delete in ('c', 'change'):
            batch_size = input('\nChange batch size: ')
            batch_size = int(batch_size)

        batch_size = batch_size_validation(items, batch_size)

        if items == 0 or batch_size == 0:
            break

    menu(settings)


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


# def parse_yaml(env_file):
#     with open(env_file) as file:
#         settings = yaml.full_load(file)
#     return settings


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

    recovery_points = list_recovery_points_by_backup_vault(
        settings, vault_name, backup_id, start_date)

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
    for cluster in clusters:
        print(preview_string(cluster, db_cluster=True))

    snapshots = describe_db_cluster_snapshots(settings)
    print(f'\nFound %s snapshots. (Max of 100 found at a time).' % (len(snapshots)))

    automated_backups = describe_db_instance_automated_backups(client)
    print(f'\nFound %s automated backups. (Max of 100 found at a time).' % (len(automated_backups)))

    #TODO: Pass snapshot/automated_backups,clusters through the menu
    #to not create additional calls which are not needed
    menu(settings, db=True)


def main(argv):

    inputfile = ''
    try:
        opts, args = getopt.getopt(argv, "hmi:", ["ifile="])
    except getopt.GetoptError:
        print('backup_cleaner.py -i <environmentfile>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('backup_cleaner.py -i dev.yml')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
            settings = parse_yaml(inputfile)
        elif opt == '-m':
            global multiprocess
            multiprocess = True

    if not inputfile:
        print('''
              \nERROR: Please create a yaml file. ex. dev.yml and call the function with the -i argument.
              \nex. python3 backup_cleaner.py -i dev.yml
              ''')
        quit()

    menu(settings)


if __name__ == "__main__":
    main(sys.argv[1:])
