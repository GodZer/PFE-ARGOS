from aws_cdk import *
from constructs import Construct
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_glue_alpha as glue_alpha
import aws_cdk.aws_iam as iam
import aws_cdk.aws_sagemaker as sgm
import os

class InnerFunction(Construct):

    def __init__(self, scope: Construct, construct_id: str, *kwargs, deliveryBucket: s3.Bucket, glueTable: glue_alpha.Table, glueDatabase: glue_alpha.Database) -> None:
        super().__init__(scope, construct_id)

        #Step functions Definition

        glue_job=glue_alpha.Job(self, "PythonShellJob",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V3_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.AssetCode.from_asset(os.path.dirname(os.path.abspath(__file__)) + '/glueETLObject2Vec/glueETLObject2Vec.py'),
            )            
        )

        outputBucket=s3.Bucket(self, 'OutputBucket',
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        outputBucket.grant_read_write(glue_job)

        deliveryBucket.grant_read(glue_job)

        glueTable.grant_read(glue_job)

        step1 = sft.GlueStartJobRun(self, "GlueJobObject2Vec", 
             glue_job_name=glue_job.job_name,
             integration_pattern=sf.IntegrationPattern.RUN_JOB,
             arguments=sf.TaskInput.from_object(
                {
                    "username": sf.JsonPath.string_at("$.partition"),
                    "bucket_name": outputBucket.bucket_name,
                    "database_name": glueDatabase.database_name,
                    "table_name": glueTable.table_name
                }
            )
        )

        #Create Chain        

        template=step1

        #Create state machine

        sm=sf.StateMachine(self, "trainingWorkflow", definition=template)