import aws_cdk as cdk
from aws_cdk import (
    aws_iam as iam,
    aws_glue as glue,
)
import projet.projet_stack


class Roles():
    def __init__(self):

        # Roles definition
        firehoseDeliveryRole = iam.Role(
            self,
            role_name="deliveryRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com")
        )
        # Link Bucket to Kinesis
        projet.projet_stack.ProjetStack.deliveryBucket.grantReadWrite(
            firehoseDeliveryRole),

        # Role linked to glue for Data management
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
