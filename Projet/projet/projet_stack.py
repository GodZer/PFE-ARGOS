# import this
from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as _s3,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as eventsource,
)
from projet.firehose import firehose
from projet.roles import firehoseDeliveryRole


class ProjetStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # LifecycleRules ? a def (exemple : 30j)
        # other parameters by default
        deliveryBucket = _s3.Bucket(
            self,
            bucket_name="ProjectBucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        currentStack = Stack.of(self)

        my_lambda = _lambda.Function(
            self,
            "InjectionFunctionHandler",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("lambda"),
            handler="injectionFunction.handler",
        )
        # Link role to lambda function:
        my_lambda.grantReadWrite(firehoseDeliveryRole)

        # Trigger lambda from Kinesis:
        eventSource = eventsource.KinesisEventSource(
            firehose,
            # Parameters ?
        )

        queue = sqs.Queue(
            self,
            "ProjetQueue",
            visibility_timeout=Duration.seconds(300),
        )

        topic = sns.Topic(self, "ProjetTopic")

        topic.add_subscription(subs.SqsSubscription(queue))
