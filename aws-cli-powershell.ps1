
$DatabaseName = "target-database-name"
$ProjectID = "project-id"

$AccountName = "my-account-name"
$ServiceName = "my-ecs-service-name"

# ex: sg-000000000000
$SecurityGroup = "security_group_id "
$SubnetOne = "subnet-somethinghere"
$SubnetTwo = "subnet-twosomething"
$AssignPublicIp = "DISABLED"

$FullServiceName = $ServiceName +'-'+ $AccountName

$ServiceDetails = @(aws ecs describe-services --cluster $AccountName --services $ServiceName)
$ServiceDetailsJson = $ServiceDetails | ConvertFrom-Json

$TaskDefinitionArn = $ServiceDetailsJson.services.taskDefinition

# You will mave to make this '- 2' or '- 3' if the task definition
# revision number goes to the teens or hundreds
$TaskDefinitionNumber = $TaskDefinitionArn.Remove(0, ($TaskDefinitionArn.Length - 1))

$network = @{
    awsvpcConfiguration = @{
        assignPublicIp = $AssignPublicIp
        securityGroups = @(
            $SecurityGroup
        )
        subnets = @(
            $SubnetOne
            $SubnetTwo
        )
    }
}


$override = @{
    containerOverrides = @(
        @{
            name = $FullServiceName
            command = @(
                $DatabaseName
                $ProjectID
            )
        }
    )
}

$NetworkJson = $network | ConvertTo-Json -Compress
$OverrideJson = $override | ConvertTo-Json -Depth 4 -Compress

$NetworkJsonEscaped = $NetworkJson -replace '\\', '\\' -replace '"', '\"'
$OverrideJsonEscaped = $OverrideJson -replace '\\', '\\' -replace '"', '\"'
$TaskDefinition = $FullServiceName +":"+ $TaskDefinitionNumber

echo $NetworkJsonEscaped
echo $OverrideJsonEscaped

# echo $TaskDefinition
# aws ecs run-task --cluster $AccountName --task-definition $TaskDefinition --launch-type=FARGATE --network-configuration $NetworkJsonEscaped --overrides $OverrideJsonEscaped
