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
import aws_cdk.aws_lambda_python_alpha as _lambda
import os
import aws_cdk.aws_apigatewayv2_alpha as apigwv2
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration

class ARGOS_STACK(Stack):

    def __init__(self, scope: Construct, construct_id: str, cloudwatch_log_group_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        log_group = logs.LogGroup.from_log_group_arn(self, "eksLogGroup", cloudwatch_log_group_arn)

        ingestionLambda = _lambda.PythonFunction(self, "IngestionLambda", 
            entry=os.path.dirname(os.path.abspath(__file__)) + '/ingestionLambda',
            runtime=Runtime.PYTHON_3_9,
            handler="handler",
            index="ingestionFunction.py")

        ingestion_api = apigwv2.HttpApi(self, "IngestionApi", create_default_stage=True)

        ingestion_lambda_integration = HttpLambdaIntegration("IngestionLambdaIntegration", ingestionLambda)

        ingestion_api.add_routes(
            path="/",
            methods=[apigwv2.HttpMethod.POST],
            integration=ingestion_lambda_integration
        )

        lambda_con = CloudWatchLogsForwarder(self, "CloudWatchLogsForwarder", log_group=log_group, api_url=ingestion_api.api_endpoint)

        