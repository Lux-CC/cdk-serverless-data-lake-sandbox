#!/usr/bin/env python3

import aws_cdk as cdk

from stacks.demo_stack import DemoStack

env_EU = cdk.Environment(region="eu-central-1")
DEMO_ID = "workshop"
app = cdk.App()
# Per account/region, provide a unique demo_name per stack
DemoStack(app, f"demo-data-{DEMO_ID}", demo_name=DEMO_ID, env=env_EU).withAthena(
    include_federated_query_infra=True
)

app.synth()
