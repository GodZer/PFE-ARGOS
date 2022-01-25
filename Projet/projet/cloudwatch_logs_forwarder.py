import os
from aws_cdk import *
from aws_cdk import Stack
from constructs import Construct
from aws_cdk.aws_lambda import *
from aws_cdk.aws_apigateway import *
import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_logs as logs
import aws_cdk.aws_logs_destinations as destinations
import aws_cdk.aws_lambda_python_alpha as _lambda

class CloudWatchLogsForwarder(Construct):

    def __init__(self, scope: Construct, construct_id: str, *kwargs, log_group: logs.ILogGroup, api_url: str) -> None:
        super().__init__(scope, construct_id)

        handler = _lambda.PythonFunction(self, "CloudWatchLoagForwarder", 
            runtime=Runtime.PYTHON_3_9,
            handler="lambda_handler",
            entry=os.path.dirname(os.path.abspath(__file__)) + '/cloudwatchLogsForwarder',
            index="messageExtraction.py",
            environment={
                "API_URL": api_url
            })

        logs.SubscriptionFilter(self, "subscriptionFilter", 
            log_group=log_group, 
            destination=destinations.LambdaDestination(handler),
            filter_pattern=logs.FilterPattern.all_events())