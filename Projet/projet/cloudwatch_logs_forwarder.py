from importlib.metadata import entry_points
from posixpath import dirname
import os
from aws_cdk import *
from aws_cdk import Stack
from constructs import Construct
from aws_cdk.aws_lambda import *
from aws_cdk.aws_apigateway import *
import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_logs as logs
import aws_cdk.aws_logs_destinations as destinations

class CloudWatchLogsForwarder(Construct):

    def __init__(self, scope: Construct, construct_id: str, log_group: logs.ILogGroup,**kwargs) -> None:
        super().__init__(scope, construct_id)

        handler = Function(self, "LambdaAPIGateway",
            runtime=Runtime.PYTHON_3_9,
            handler="messageExtraction.lambda_handler",
            code=Code.from_asset(os.path.dirname(os.path.abspath(__file__)) + '/cloudwatchLogsForwarder'))

        logs.SubscriptionFilter(self, "subscriptionFilter", 
            log_group=log_group, 
            destination=destinations.LambdaDestination(handler),
            filter_pattern=logs.FilterPattern.all_events())