from aws_cdk import Duration
from constructs import Construct
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_lambda_python_alpha as _lambda
import os
from aws_cdk.aws_lambda import Runtime
import aws_cdk.aws_glue_alpha as _glue
import aws_cdk.aws_iam as iam
from .innerFunction import InnerFunction


class TrainingStepFunction(Construct):
    def __init__(self, scope: Construct, id: str, *, crawler_name: str, inner_function: InnerFunction, table: _glue.Table, database: _glue.Database) -> None:
        super().__init__(scope, id)

        partition_retriever_lambda = _lambda.PythonFunction(self, "PartitionRetriever",
                                        entry=os.path.dirname(os.path.abspath(__file__)) + '/lambdaPartitionRetriever',
                                        runtime=Runtime.PYTHON_3_9,
                                        handler="lambda_handler",
                                        index="lambdaPartitionRetriever.py",
                                        environment={
                                                    "DATABASE_NAME": database.database_name,
                                                    "TABLE_NAME": table.table_name
                                                    },
                                        timeout=Duration.seconds(30)
                                        )

        table.grant_read(partition_retriever_lambda)
        partition_retriever_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["glue:getPartitions"],
            resources=[database.catalog_arn, database.database_arn])
        )

        partition_retriever_lambda_task=sft.LambdaInvoke(self, "PartitionRetrieverLambdaTask", 
            lambda_function=partition_retriever_lambda,
            integration_pattern=sf.IntegrationPattern.REQUEST_RESPONSE,
            result_selector={
                "partitions": sf.JsonPath.list_at("$.Payload")
            }
        )

        map_task=sf.Map(self, "TrainingLoop", 
            items_path=sf.JsonPath.string_at("$.partitions"),
            max_concurrency=1
        )

        run_training_for_user=sft.StepFunctionsStartExecution(self, "TrainingForUser", 
            state_machine=inner_function.sm,
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )

        map_task.iterator(run_training_for_user)

        template=partition_retriever_lambda_task.next(map_task)

        sm=sf.StateMachine(self, "TrainingStateMachine", definition=template)


