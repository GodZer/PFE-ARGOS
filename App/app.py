#!/usr/bin/env python3
import aws_cdk as cdk
from stack.ARGOS_STACK import ARGOS_STACK
import parameters

app = cdk.App()
ARGOS_STACK(app, "ARGOS", cloudwatch_log_group_arn=parameters.cloudwatch_loggroup_arn)

app.synth()