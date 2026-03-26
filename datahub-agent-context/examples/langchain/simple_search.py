#!/usr/bin/env python3
"""
Minimal example: Simple dataset search with LangChain 1.x using AWS Bedrock.

This is the absolute minimum code needed to use DataHub tools with LangChain.

Prerequisites:
    - AWS credentials configured (via ~/.aws/credentials or environment variables)
    - Access to Claude 3.5 Haiku in AWS Bedrock

Usage:
    python simple_search.py
"""

import os

from langchain.agents import create_agent

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools

# 1. Connect to DataHub
datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
if datahub_gms_url is None:
    client = DataHubClient.from_env()
else:
    client = DataHubClient(server=datahub_gms_url, token=os.getenv("DATAHUB_GMS_TOKEN"))

# 2. Create tools using the builder (read-only tools)
tools = build_langchain_tools(client, include_mutations=False)


# 3. Create agent using LangChain 1.x create_agent API with Bedrock
import boto3
from langchain_aws import ChatBedrock

bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name="us-west-2",  # Change to your region
)

model = ChatBedrock(
    client=bedrock_runtime,
    model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    model_kwargs={"max_tokens": 2048, "temperature": 0},
)

agent = create_agent(
    model,
    tools=tools,
    system_prompt="You help users find datasets in DataHub. Provide clear, concise answers.",
)

# 4. Run a query
if __name__ == "__main__":
    response = agent.invoke(
        {"messages": [{"role": "user", "content": "Find datasets about customers"}]}
    )
    final_message = response["messages"][-1]
    print(f"\n🤖 Agent: {final_message.content}")
