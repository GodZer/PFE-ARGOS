import aws_cdk as cdk
from aws_cdk import (
    aws_iam as iam,
    aws_glue as glue,
    aws_kinesisfirehose as firehose,
)
from projet.projet_stack import (
    currentStack,
    deliveryBucket
)
from projet.roles import (
    firehoseDeliveryRole,
    firehoseSchemaConfigurationRole
)


class Role():
    def __init__(self):
        # Kinesis Firehose :
        firehose.CfnDeliveryStream(
            self,
            "trailDeliveryStream",
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=deliveryBucket.bucketArn,
                role_arn=firehoseDeliveryRole.roleArn,
                prefix="username=!{partitionKeyFromQuery:user}/",
                error_output_prefix="ingestionError/",
                buffering_hints=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                    interval_in_seconds=cdk.Duration.minutes(
                        5).to_seconds(),  # 300
                    size_in_mBs=cdk.Size(
                        cdk.mebibytes(128).toMebibytes()),  # 128
                )
            ),
            data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                enabled=True,
                input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                    deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                        # je sais pas si c'est la bonne manière d'écrire
                        hive_json_ser_de=firehose.CfnDeliveryStream.HiveJsonSerDeProperty(),
                    )
                ),
            ),
            output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                serializer=firehose.CfnDeliveryStream.SerializerProperty(
                    parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(
                        compression="UNCOMPRESSED",
                        writer_version="V2"
                    )
                )
            ),
            schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                database_name=glue.database.databaseName,  # changer database
                catalog_id=glue.database.catalogId,  # changer database
                region=currentStack.region,
                role_arn=firehoseSchemaConfigurationRole.roleArn,
                table_name=glue.table.tableName,
            ),
            processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                enabled=True,
                processors=[firehose.CfnDeliveryStream.ProcessorProperty(
                    type="MetadataExtraction",
                    parameters=[{firehose.CfnDeliveryStream.ProcessorParameterProperty(
                        parameter_name='MetadataExtractionQuery',
                        parameter_value='{user: .user}',
                    )},
                        {firehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name='JsonParsingEngine',
                            parameter_value='JQ-1.6',
                        )}
                    ]

                )]
            ),
            dynamic_partitioning_configuration=firehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                enabled=True,
            )
        )
