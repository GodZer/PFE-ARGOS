import aws_cdk as cdk
from aws_cdk import (
    aws_glue as glue,
    aws_kinesisfirehose as kinesisfirehose,
)
import projet.roles
import projet.projet_stack


class Firehose():
    def __init__(self):

        # Kinesis Firehose :
        firehose = kinesisfirehose.CfnDeliveryStream(
            self,
            "trailDeliveryStream",
            extended_s3_destination_configuration=kinesisfirehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=projet.projet_stack.ProjetStack.deliveryBucket.bucketArn,
                role_arn=projet.roles.Roles.firehoseDeliveryRole.roleArn,
                prefix="username=!{partitionKeyFromQuery:user}/",
                error_output_prefix="ingestionError/",
                buffering_hints=kinesisfirehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                    interval_in_seconds=cdk.Duration.minutes(
                        5).to_seconds(),  # 300
                    size_in_mBs=cdk.Size(
                        cdk.mebibytes(128).toMebibytes()),  # 128
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
                region=projet.projet_stack.ProjetStack.currentStack.region,
                role_arn=projet.roles.Roles.firehoseSchemaConfigurationRole.roleArn,
                table_name=glue.table.tableName,
            ),
            processing_configuration=kinesisfirehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                enabled=True,
                processors=[kinesisfirehose.CfnDeliveryStream.ProcessorProperty(
                    type="MetadataExtraction",
                    parameters=[{kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                        parameter_name='MetadataExtractionQuery',
                        parameter_value='{user: .user}',
                    )},
                        {kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name='JsonParsingEngine',
                            parameter_value='JQ-1.6',
                        )}
                    ]

                )]
            ),
            dynamic_partitioning_configuration=kinesisfirehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                enabled=True,
            )
        )
