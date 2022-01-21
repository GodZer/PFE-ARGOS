from importlib.metadata import entry_points
from posixpath import dirname
from aws_cdk import *
from aws_cdk import Stack
from constructs import Construct
from aws_cdk.aws_lambda import *
from aws_cdk.aws_apigateway import *
import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_logs as logs
import aws_cdk.aws_logs_destinations as destinations
from .cloudwatch_logs_forwarder import CloudWatchLogsForwarder

class ARGOS_STACK(Stack):

    def __init__(self, scope: Construct, construct_id: str, cloudwatch_log_group_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        log_group = logs.LogGroup.from_log_group_arn(self, "eksLogGroup", cloudwatch_log_group_arn)

        lambda_con = CloudWatchLogsForwarder(self, "CloudWatchLogsForwarder", log_group=log_group, api_url="https://vkcdwvizij.execute-api.eu-west-3.amazonaws.com/test")

        