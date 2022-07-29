import sys
import yaml
import boto3
import json
from os import path


def parse_yaml(env_file):
    with open(env_file) as file:
        settings = yaml.full_load(file)
    return settings


def run_task(settings, override):

    sts_client = boto3.client('sts')

    assumed_role_object = sts_client.assume_role(
        RoleArn=settings['role'],
        RoleSessionName='ecs-run-task-script'
    )

    credentials = assumed_role_object['Credentials']

    client = boto3.client(
        'ecs',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    cluster = settings['cluster_name']
    print(cluster)
    application_name = settings['application_name']
    deployment_name = settings['deployment_name']

    if settings['ignore_service_env']:
        service_name = application_name
    else:
        service_name = f'{application_name}-{deployment_name}'

    if settings['ignore_task_def_env']:
        task_def_name = application_name
    else:
        task_def_name = f'{application_name}-{deployment_name}'

    service_description = client.describe_services(
        cluster=cluster,
        services=[
            service_name,
        ]
    )

    subnets = service_description['services'][0]['deployments'][0]\
                    ['networkConfiguration']['awsvpcConfiguration']\
                    ['subnets']
    security_groups = service_description['services'][0]['deployments']\
            [0]['networkConfiguration']['awsvpcConfiguration']\
            ['securityGroups']

    # Get the current running task definition revision
    current_task_definition = client.describe_task_definition(
        taskDefinition=task_def_name)

    current_revision = current_task_definition['taskDefinition']['revision']
    task_definition = f'{task_def_name}:{current_revision}'

    response = client.run_task(
        cluster=cluster,
        count=settings['count'],
        enableECSManagedTags=True,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': subnets,
                'securityGroups': security_groups,
                'assignPublicIp': 'DISABLED'
            }
        },
        overrides={
            'containerOverrides': [
                {
                    'name': task_def_name,
                    'command': override,

                }
            ]
        },
        platformVersion='1.4.0',
        startedBy='ecs-run-task-script',
        taskDefinition=task_definition
    )

    return response


def output_details(response, settings):

    cluster_arn = response['tasks'][0]['clusterArn']
    container_arn = response['tasks'][0]['containers'][0]['containerArn']
    image = response['tasks'][0]['containers'][0]['image']
    last_status = response['tasks'][0]['containers'][0]['lastStatus']
    desired_status = response['tasks'][0]['desiredStatus']
    platform = response['tasks'][0]['platformVersion']
    overrides = response['tasks'][0]['overrides']['containerOverrides']

    json_data = {
        "clusterArn": cluster_arn,
        "containerArn": container_arn,
        "image": image,
        "lastStatus": last_status,
        "desiredStatus": desired_status,
        "platform": platform,
    }

    for override in overrides:
        if 'command' in override:
            name = override['name']
            command = override['command']
            json_data['overrides'] = {name: command}

    print(json.dumps(json_data, indent=2))

    print(response)


if __name__ == "__main__":

    arg = sys.argv[1:]
    if len(arg) < 2:
        print('Please provide a deployment environment and the task override for tendenci. ex. python3 setup.py staging migrate')
        quit()
    override = str(arg[1]).split()

    print(override)

    env_file = str(arg[0]) + '.yml'
    if not path.exists(env_file):
        print('Please create a yaml file. ex. ' + env_file)
        quit()
    settings = parse_yaml(env_file)
    response = run_task(settings, override)
    output_details(response, settings)
