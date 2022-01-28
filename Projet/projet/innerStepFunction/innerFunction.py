from aws_cdk import *
from constructs import Construct
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_glue_alpha as glue_alpha
import aws_cdk.aws_iam as iam
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

        datasetStorageBucket=s3.Bucket(self, 'DatasetStorageBucket',
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        modelStorageBucket=s3.Bucket(self, 'ModelStorageBucket',
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        valueCalculator = sf.Pass(self, "PathCalculator", 
            parameters={
                "partition.$": "$.partition",
                "object2vec_training_dataset_path.$": f"States.Format('s3://{datasetStorageBucket.bucket_name}/{{}}/training_O2V/', $.partition)",
                "object2vec_inference_dataset_path.$": f"States.Format('s3://{datasetStorageBucket.bucket_name}/{{}}/ingestion_transform/', $.partition)",
                "object2vec_inference_output_path.$": f"States.Format('s3://{datasetStorageBucket.bucket_name}/{{}}/ingestion_transform_output/', $.partition)",
                "object2vec_model_path.$": f"States.Format('s3://{modelStorageBucket.bucket_name}/{{}}/model_O2V/', $.partition)",
                "rcf_model_path.$": f"States.Format('s3://{modelStorageBucket.bucket_name}/{{}}/model_RCF/', $.partition)",
                "rcf_training_dataset_path.$": f"States.Format('s3://{datasetStorageBucket.bucket_name}/{{}}/training_RCF/', $.partition)"
            }
        )

        

        datasetStorageBucket.grant_read_write(glue_job)

        deliveryBucket.grant_read(glue_job)

        glueTable.grant_read(glue_job)

        glueJobObject2Vec = sft.GlueStartJobRun(self, "GlueJobObject2Vec", 
             glue_job_name=glue_job.job_name,
             integration_pattern=sf.IntegrationPattern.RUN_JOB,
             arguments=sf.TaskInput.from_object(
                {
                    "--username": sf.JsonPath.string_at("$.partition"),
                    "--bucket_name": datasetStorageBucket.bucket_name,
                    "--database_name": glueDatabase.database_name,
                    "--table_name": glueTable.table_name
                }
            ),
            result_path=sf.JsonPath.DISCARD
        )

        sagemaker_role=iam.Role(self, "SageMakerRole", 
             assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com")
        )

        sagemaker_role.add_managed_policy(iam.ManagedPolicy.from_managed_policy_arn(self, "AmazonSageMakerFullAccess", "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"))

        sagemaker_role.add_managed_policy(iam.ManagedPolicy.from_managed_policy_arn(self, "S3FullAccess", "arn:aws:iam::aws:policy/AmazonS3FullAccess"))

        datasetStorageBucket.grant_read(sagemaker_role)
        modelStorageBucket.grant_read_write(sagemaker_role)

        trainingObject2Vec = sft.SageMakerCreateTrainingJob(self, "CreateObject2VecTrainingJob",
        role=sagemaker_role,
        algorithm_specification=sft.AlgorithmSpecification(
            training_image=sft.DockerImage.from_registry("749696950732.dkr.ecr.eu-west-3.amazonaws.com/object2vec:1"),
            training_input_mode=sft.InputMode.FILE
        ),
        input_data_config=[sft.Channel(
            channel_name="train",
            data_source=sft.DataSource(
                s3_data_source=sft.S3DataSource(
                    s3_location=sft.S3Location.from_json_expression("$.object2vec_training_dataset_path")
                )
            )
        )
        ],
        output_data_config=sft.OutputDataConfig(
            s3_output_location=sft.S3Location.from_json_expression("$.object2vec_model_path")
        ),
        training_job_name=sf.JsonPath.string_at("$$.Execution.Name"),
        hyperparameters={
            "dropout": "0.2",
            "enc0_max_seq_len": "10",
            "enc0_network": "bilstm",
            "enc0_token_embedding_dim": "10",
            "enc0_vocab_size": "2097152",
            "enc_dim": "10",
            "epochs": "4",
            "learning_rate": "0.01",
            "mini_batch_size": "4096",
            "mlp_activation": "relu",
            "mlp_dim": "512",
            "mlp_layers": "2",
            "output_layer": "mean_squared_error",
            "tied_token_embedding_weight": "true", 
        },
        integration_pattern=sf.IntegrationPattern.RUN_JOB,
        resource_config=sft.ResourceConfig(instance_count=1, instance_type=ec2.InstanceType("m5.4xlarge"), volume_size=Size.gibibytes(500)),
        result_selector={
            "model_path": sf.JsonPath.string_at("$.ModelArtifacts.S3ModelArtifacts")
        },
        result_path="$.training_O2V"
        )

        trainingObject2Vec.role.attach_inline_policy(iam.Policy(self, "GrantPassRole", document=iam.PolicyDocument(
            statements=[iam.PolicyStatement(actions=["iam:PassRole"], resources=[sagemaker_role.role_arn])]
        )))

        createObject2VecModel= sft.SageMakerCreateModel(self, "CreateObject2VecModel",
            model_name=sf.JsonPath.string_at("$$.Execution.Name"),
            primary_container= sft.ContainerDefinition(
                image=sft.DockerImage.from_registry("749696950732.dkr.ecr.eu-west-3.amazonaws.com/object2vec:1"),
                model_s3_location=sft.S3Location.from_json_expression("$.training_O2V.model_path")
            ),
            result_selector={
                 "model_arn": sf.JsonPath.string_at("$.ModelArn"),
                 "model_name": sf.JsonPath.string_at("$$.Execution.Name")
            },
            result_path="$.object2vec_model"
        )

        datasetStorageBucket.grant_read_write(createObject2VecModel)
        modelStorageBucket.grant_read(createObject2VecModel)

        batchTransformJob=sft.SageMakerCreateTransformJob(self, "BatchTransformJob",
            transform_job_name=sf.JsonPath.string_at("$$.Execution.Name"),
            model_name=sf.JsonPath.string_at("$.object2vec_model.model_name"),
            transform_input=sft.TransformInput(
                transform_data_source=sft.TransformDataSource(
                    s3_data_source=sft.TransformS3DataSource(
                        s3_uri=sf.JsonPath.string_at("$.object2vec_inference_dataset_path"),
                        s3_data_type=sft.S3DataType.S3_PREFIX
                    )
                )
            ),
            transform_output=sft.TransformOutput(
                s3_output_path=sf.JsonPath.string_at("$.object2vec_inference_output_path")
            ),
            transform_resources=sft.TransformResources(instance_count=1, instance_type=ec2.InstanceType("m5.4xlarge")),
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )
        
        #Create Chain        

        template=valueCalculator.next(glueJobObject2Vec).next(trainingObject2Vec).next(createObject2VecModel).next(batchTransformJob)

        #Create state machine

        sm=sf.StateMachine(self, "trainingWorkflow", definition=template)

        datasetStorageBucket.grant_read_write(sm)
        modelStorageBucket.grant_read(sm)