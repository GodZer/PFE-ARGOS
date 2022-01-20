# import this
from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as _s3,
    aws_glue as glue,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_sns_subscriptions as subs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as EventSource,
    aws_kinesisfirehose as kinesisfirehose,
)


class ProjetStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # LifecycleRules ? a def (exemple : 30j)
        # other parameters by default
        deliveryBucket = _s3.Bucket(
            self,
            "ProjectBucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        currentStack = Stack.of(self)

        # Roles definition
        firehoseDeliveryRole = iam.Role(
            self,
            "deliveryRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com")
        )
        # Role assign between Firehose and Bucket
        deliveryBucket.grant_read_write(
            firehoseDeliveryRole),

        # Role linked to glue for Data management
        """
        firehoseSchemaConfigurationRole = iam.Role(
            self,
            role_name="schemaConfigurationRole",
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
                                glue.table.tableArn, glue.database.catalogArn, glue.database.databaseArn]  # variable database et table
                        )
                    ]
                )
            }
        )
        """
        # Lamda function
        my_lambda = _lambda.Function(
            self,
            "InjectionFunctionHandler",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("lambda"),
            handler="injectionFunction.handler",
        )

        # Links lambda to Kinesis
        kinesis_stream = kinesis.Stream(self, "MyStream")

        # Upgrade permissions to use Stream:
        kinesis_stream.grant_write(
            my_lambda)

        # Create Kinesis Event Source:
        kinesis_event_source = EventSource.KinesisEventSource(
            stream=kinesis_stream,
            starting_position=_lambda.StartingPosition.LATEST,  # à voir si c'est le bon
        )

        # Trigger lambda from Kinesis:
        my_lambda.add_event_source(kinesis_event_source)

        """
        # Links Kinesis to S3 Bucket
        firehose = kinesisfirehose.CfnDeliveryStream(
            self,
            "trailDeliveryStream",
            extended_s3_destination_configuration=kinesisfirehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=deliveryBucket.bucket_arn,
                role_arn=firehoseDeliveryRole.role_arn,
                prefix="username=!{partitionKeyFromQuery:user}/",
                error_output_prefix="ingestionError/",
                buffering_hints=kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=cdk.Duration.minutes(
                        5).to_seconds(),  # 300
                    size_in_mBs=128,  # 128  !! erreur!!
                )
            ),
             data_format_conversion_configuration=kinesisfirehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
             enabled=True,
             input_format_configuration=kinesisfirehose.CfnDeliveryStream.InputFormatConfigurationProperty(
             deserializer=kinesisfirehose.CfnDeliveryStream.DeserializerProperty(
             hive_json_ser_de=kinesisfirehose.CfnDeliveryStream.HiveJsonSerDeProperty(),
             )
             ),
             ),
             output_format_configuration=kinesisfirehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
             serializer=kinesisfirehose.CfnDeliveryStream.SerializerProperty(
             parquet_ser_de=kinesisfirehose.CfnDeliveryStream.ParquetSerDeProperty(
             compression="UNCOMPRESSED",
             writer_version="V2"
             )
             )
             ),

             schema_configuration=kinesisfirehose.CfnDeliveryStream.SchemaConfigurationProperty(
             database_name=glue.database.databaseName,  # changer database
             catalog_id=glue.database.catalogId,  # changer database
             region=currentStack.region,
             role_arn=firehoseSchemaConfigurationRole.roleArn,
             table_name=glue.table.tableName,
             ),

             processing_configuration=kinesisfirehose.CfnDeliveryStream.ProcessingConfigurationProperty(
             enabled=True,
             processors=[kinesisfirehose.CfnDeliveryStream.ProcessorProperty(
             type="MetadataExtraction",
             parameters=[
             (kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
             parameter_name='MetadataExtractionQuery',
            parameter_value='{user: .user}',
             )),
             (kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
             parameter_name='JsonParsingEngine',
             parameter_value='JQ-1.6',
             ))
             ]

             )]
             ),
             dynamic_partitioning_configuration=kinesisfirehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
             enabled=True,
             )
        )
        """

        queue = sqs.Queue(
            self,
            "ProjetQueue",
            visibility_timeout=Duration.seconds(300),
        )

        topic = sns.Topic(self, "ProjetTopic")

        topic.add_subscription(subs.SqsSubscription(queue))
