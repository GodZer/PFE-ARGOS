from aws_cdk import *
from aws_cdk import Stack
from constructs import Construct
from aws_cdk.aws_lambda import *
from aws_cdk.aws_apigateway import *
import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_logs_destinations as destinations
from .innerStepFunction.trainingStepFunction import TrainingStepFunction
from .innerStepFunction.innerFunction import InnerFunction
from .cloudwatch_logs_forwarder import CloudWatchLogsForwarder
import aws_cdk.aws_lambda_python_alpha as _lambda
import os
import aws_cdk.aws_apigatewayv2_alpha as apigwv2
import aws_cdk.aws_glue_alpha as glue_alpha
import aws_cdk.aws_glue as glue
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration
import aws_cdk.aws_kinesisfirehose as firehose
import aws_cdk.aws_iam as iam

class ARGOS_STACK(Stack):

    def __init__(self, scope: Construct, construct_id: str, cloudwatch_log_group_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        log_group = logs.LogGroup.from_log_group_arn(self, "eksLogGroup", cloudwatch_log_group_arn)

        deliveryBucket = s3.Bucket(self, "deliveryBucket", auto_delete_objects=True, removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(21))
            ]
        )

        glue_database = glue_alpha.Database(self, "argos", database_name="argos")
        table = glue_alpha.Table(self, "k8s_audit",
            database=glue_database,
            table_name="k8s_audit",
            columns=[
                glue_alpha.Column(name="verb", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="groups", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="userAgent", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="sourceIPs", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="resource", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="subresource", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="name", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="apiGroup", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="namespace", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="impersonatedUser", type=glue_alpha.Schema.STRING),
                glue_alpha.Column(name="encodeur", type=glue_alpha.Schema.array(input_string="bigint", is_primitive=True))
            ],
            partition_keys=[
                glue_alpha.Column(name="username", type=glue_alpha.Schema.STRING)
            ],
            data_format=glue_alpha.DataFormat.PARQUET,
            bucket=deliveryBucket)

        table.add_partition_index(key_names=["username"])

        # Roles definition
        firehoseDeliveryRole = iam.Role(
            self,
            "firehoseDeliveryRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com")
        )
        # Role assign between Firehose and Bucket
        deliveryBucket.grant_read_write(
            firehoseDeliveryRole)

        # Role linked to glue for Data management
        
        firehoseSchemaConfigurationRole = iam.Role(
            self,
            "firehoseSchemaConfigurationRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            inline_policies={
                'glueAccess': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "glue:GetTable",
                                "glue:GetTableVersion",
                                "glue:GetTableVersions"
                            ],
                            resources=[
                                table.table_arn, glue_database.catalog_arn, glue_database.database_arn]  # variable database et table
                        )
                    ]
                )
            }
        )

        delivery_stream = firehose.CfnDeliveryStream(self, "deliveryStream",
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=deliveryBucket.bucket_arn,
                role_arn=firehoseDeliveryRole.role_arn,
                prefix="username=!{partitionKeyFromQuery:username}/",
                error_output_prefix="ingestionError/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=Duration.minutes(5).to_seconds(),
                    size_in_m_bs=Size.mebibytes(128).to_mebibytes()
                ),
                data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                    enabled=True,
                    input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                        deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                            hive_json_ser_de=firehose.CfnDeliveryStream.HiveJsonSerDeProperty()
                        )
                    ),
                    output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                        serializer=firehose.CfnDeliveryStream.SerializerProperty(
                            parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(
                                compression="SNAPPY",
                                writer_version="V2"
                            )
                        )
                    ),
                    schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                        catalog_id=glue_database.catalog_id,
                        database_name=glue_database.database_name,
                        region=self.region,
                        role_arn=firehoseSchemaConfigurationRole.role_arn,
                        table_name=table.table_name
                    ),
                ),
                processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[
                        firehose.CfnDeliveryStream.ProcessorProperty(
                            type="MetadataExtraction",
                            parameters=[
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="MetadataExtractionQuery",
                                    parameter_value="{username: .username}"
                                ),
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="JsonParsingEngine",
                                    parameter_value="JQ-1.6"
                                )
                            ]
                        )
                    ]
                ),
                dynamic_partitioning_configuration=firehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                    enabled=True
                )
            )
        )

        ingestionLambda = _lambda.PythonFunction(self, "IngestionLambda", 
            entry=os.path.dirname(os.path.abspath(__file__)) + '/ingestionLambda',
            runtime=Runtime.PYTHON_3_9,
            handler="handler",
            index="ingestionFunction.py",
            environment={
                "FIREHOSE_DELIVERY_STREAM_NAME": delivery_stream.ref
            })

        ingestionLambda.role.attach_inline_policy(iam.Policy(self, "ingestionLambdaKinesisPolicy", document=iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "firehose:PutRecord",
                    ],
                    resources=[
                        delivery_stream.attr_arn
                    ]  # variable database et table
                )
            ]
        )))

        ingestionLambda.role.attach_inline_policy(
            iam.Policy(
                self,
                "AllowInvokeEndpoint",
                document=iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        actions=[
                            "sagemaker:invokeEndpoint"
                        ],
                        resources=["*"]
                    )
                ]
                )
            )
        )

        ingestion_api = apigwv2.HttpApi(self, "IngestionApi", create_default_stage=True)

        ingestion_lambda_integration = HttpLambdaIntegration("IngestionLambdaIntegration", ingestionLambda)

        ingestion_api.add_routes(
            path="/",
            methods=[apigwv2.HttpMethod.POST],
            integration=ingestion_lambda_integration
        )

        lambda_con = CloudWatchLogsForwarder(self, "CloudWatchLogsForwarder", log_group=log_group, api_url=ingestion_api.api_endpoint)

        crawler_role=iam.Role(self, "CrawlerRole", 
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )

        crawler_role.add_managed_policy(iam.ManagedPolicy.from_managed_policy_arn(self, "AWSGlueServiceRole", "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"))

        deliveryBucket.grant_read(crawler_role)

        crawler=glue.CfnCrawler(self, "Crawler",
            role=crawler_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                catalog_targets=[glue.CfnCrawler.CatalogTargetProperty(
                    database_name=glue_database.database_name,
                    tables=[table.table_name]
                )]
            ),
            database_name=glue_database.database_name,
            description="Crawler",
            schedule=None,
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
                update_behavior="LOG"
            )
        
        )

        user_training_function=InnerFunction(self, "UserTrainingFunction", deliveryBucket=deliveryBucket, glueTable=table, glueDatabase=glue_database)

        training_step_function=TrainingStepFunction(self, "TrainingStepFunction", crawler_name=crawler.ref, inner_function=user_training_function, table=table, database=glue_database)

        