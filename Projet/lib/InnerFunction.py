from re import template
from constructs import Construct
from aws_cdk import aws_stepfunctions as sf
from aws_cdk.aws_stepfunctions import *
from aws_cdk import App, Stack
from aws_cdk.aws_stepfunctions_tasks import *
#from aws_cdk.aws_stepfunctions_tasks import GlueStartJobRun, SageMakerCreateTrainingJob, InputMode, S3Location
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
from aws_cdk.aws_ec2 import *
from aws_cdk import *
from aws_cdk import RemovalPolicy
#from aws_cdk.aws_stepfunctions import StateMachine

class InnerFunction(Stack):
    def __init__(self, scope:Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        #Input and output Buckets for models

        inputBucket=s3.Bucket(self, "input",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        outputBucket=s3.Bucket(self, "output",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        #Step functions Definition

        step1 = GlueStartJobRun(self, "GlueJobObject2Vec", 
            glue_job_name="glue-job-name",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )

        step2 = SageMakerCreateTrainingJob(self, "trainingObject2Vec",
        algorithm_specification=AlgorithmSpecification(
            algorithm_name="object2vec",
            training_input_mode=InputMode.FILE
        ),
        input_data_config=[Channel(
            channel_name="train",
            data_source=DataSource(
                s3_data_source=S3DataSource(
                    s3_location=S3Location.from_bucket(inputBucket, "/"
                    )
                )
            )
        )],
        training_job_name="object2vec",
        output_data_config=OutputDataConfig(
            s3_output_location=S3Location.from_bucket(outputBucket, "/"
            ),
        integration_pattern=sf.IntegrationPattern.RUN_JOB),

        resource_config=ResourceConfig(
            instance_count=1,
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.MEMORY5, ec2.InstanceSize.XLARGE2),
            volume_size=Size.gibibytes(50)
        ),  # optional: default is 1 instance of EC2 `M4.XLarge` with `10GB` volume
        stopping_condition=StoppingCondition(
            max_runtime=Duration.hours(2)
        )
        )

        #Create Chain        

        template=step1.next(step2)

        #Create state machine

        sm=sf.StateMachine(self, "trainingWorkflow", definition=template)



