#!/usr/bin/env python3
import os
import json
import aws_cdk as cdk
from stack import FireboltSchedulerStack

app = cdk.App()

config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
with open(config_path) as f:
    config = json.load(f)

FireboltSchedulerStack(
    app,
    "FireboltQueryScheduler",
    config=config,
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION", "ap-south-1"),
    ),
)

app.synth()
