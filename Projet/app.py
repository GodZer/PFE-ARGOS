#!/usr/bin/env python3

import aws_cdk as cdk

from projet.projet_stack import ProjetStack


app = cdk.App()
ProjetStack(app, "projet")

app.synth()
