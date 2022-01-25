from asyncio import Task
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

        outputBucketGlueJob=s3.Bucket(self, 'OutputBucketGlueJob',
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        outputBucketGlueJob.grant_read_write(glue_job)

        deliveryBucket.grant_read(glue_job)

        glueTable.grant_read(glue_job)

        step1 = sft.GlueStartJobRun(self, "GlueJobObject2Vec", 
             glue_job_name=glue_job.job_name,
             integration_pattern=sf.IntegrationPattern.RUN_JOB,
             arguments=sf.TaskInput.from_object(
                {
                    "--username": sf.JsonPath.string_at("$.partition"),
                    "--bucket_name": outputBucketGlueJob.bucket_name,
                    "--database_name": glueDatabase.database_name,
                    "--table_name": glueTable.table_name
                }
            )
        )

        outputBucketObject2Vec=s3.Bucket(self, 'OutputBucketObject2Vec',
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # sagemaker_role=iam.Role(self, "SageMakerRole", 
        #      assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com")
        # )

        # sagemaker_role.add_managed_policy(iam.ManagedPolicy.from_managed_policy_arn(self, "AmazonSageMakerFullAccess", "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"))

        # outputBucketGlueJob.grant_read(sagemaker_role)
        # outputBucketObject2Vec.grant_read_write(sagemaker_role)

        trainingObject2Vec = sft.SageMakerCreateTrainingJob(self, "Object2Vec",
        algorithm_specification=sft.AlgorithmSpecification(
            algorithm_name="Object2Vec",
            training_input_mode=sft.InputMode.FILE
        ),
        input_data_config=[sft.Channel(
            channel_name="train",
            data_source=sft.DataSource(
                s3_data_source=sft.S3DataSource(
                    s3_location=sft.S3Location.from_bucket(outputBucketGlueJob, "/")
                )
            )
        )
        ],
        output_data_config=sft.OutputDataConfig(
            s3_output_location=sft.S3Location.from_bucket(outputBucketObject2Vec, "/")
        ),
        training_job_name="TrainingObject2Vec",
        integration_pattern=sf.IntegrationPattern.RUN_JOB
        )

        outputBucketGlueJob.grant_read(trainingObject2Vec)
        outputBucketObject2Vec.grant_read_write(trainingObject2Vec)


        #Create Chain        

        template=step1.next(trainingObject2Vec)

        #Create state machine

        sm=sf.StateMachine(self, "trainingWorkflow", definition=template)

        #Transform job
        #sm_transformer = sgm.transformer.Transformer(model_name = "Object2Vec"
        #                   instance_count = ,
        #                   instance_type = ,
        #                   output_path = 
        #                   )