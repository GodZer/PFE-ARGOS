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

class ARGOS_STACK(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        handler = Function(self, "LambdaAPIGateway",
            runtime=Runtime.PYTHON_3_9,
            handler="messageExtraction.lambda_handler",
            code=Code.from_asset('projet'))

        #log_group = logs.LogGroup(self, "Log Group")
        
        #subscription_filter=logs.SubscriptionFilter(self, "Subscription",
        #    log_group=log_group,
        #    destination=destinations.LambdaDestination(fn),
        #    filter_pattern=logs.FilterPattern.all_terms("ERROR", "MainThread")
        #)