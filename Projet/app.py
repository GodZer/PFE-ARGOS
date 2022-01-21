#!/usr/bin/env python3
import aws_cdk as cdk
from projet.ARGOS_STACK import ARGOS_STACK

app = cdk.App()
ARGOS_STACK(app, "ARGOS")

app.synth()