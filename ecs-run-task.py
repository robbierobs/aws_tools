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

    cluster = 'fargate-' + settings['deploymentName']
    service_name = settings['service_name']
    container_name = service_name + '-' + settings['deploymentName']
    task_definition = service_name + '-' + settings['deploymentName'] + ':' + str(settings['taskDefinitionRev'])

    response = client.run_task(
        cluster=cluster,
        count=settings['count'],
        enableECSManagedTags=True,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    settings['subnets'],
                ],
                'securityGroups': [
                    settings['securityGroup'],
                ],
                'assignPublicIp': 'DISABLED'
            }
        },
        overrides={
            'containerOverrides': [
                {
                    'name': container_name,
                    'command': [
                        override,
                    ]
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
        print('Please provide a deployment environment and the task override for the service. ex. python3 setup.py staging migrate')
        quit()
    override = str(arg[1])
    env_file = str(arg[0]) + '.yml'
    if not path.exists(env_file):
        print('Please create a yaml file. ex. ' + env_file)
        quit()
    settings = parse_yaml(env_file)
    response = run_task(settings, override)
    output_details(response, settings)

