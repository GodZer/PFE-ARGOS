from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_lambda as _lambda,
)


class ProjetStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        my_lambda = _lambda.Function(
            self,
            "HelloHandler",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset("lambda"),
            handler="injectionFunction.handler",
        )

        queue = sqs.Queue(
            self,
            "ProjetQueue",
            visibility_timeout=Duration.seconds(300),
        )

        topic = sns.Topic(self, "ProjetTopic")

        topic.add_subscription(subs.SqsSubscription(queue))
