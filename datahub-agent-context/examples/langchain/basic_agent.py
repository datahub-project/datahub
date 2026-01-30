#!/usr/bin/env python3
"""
Basic LangChain 1.x agent example using DataHub MCP tools with AWS Bedrock.

This example demonstrates how to create a simple LangChain agent that can:
- Search for datasets in DataHub
- Get detailed entity information
- Query lineage relationships
- Update metadata like tags and descriptions

Prerequisites:
    pip install langchain>=1.0 langchain-aws boto3 datahub-agent-context
    AWS credentials configured (via ~/.aws/credentials or environment variables)
    Access to Claude 3.5 Haiku in AWS Bedrock

Environment variables:
    AWS_REGION: AWS region for Bedrock (default: us-west-2)
    DATAHUB_GMS_URL: DataHub GMS endpoint (default: http://localhost:8080)
    DATAHUB_GMS_TOKEN: DataHub access token (optional)
"""

import os

import boto3
from langchain.agents import create_agent
from langchain_aws import ChatBedrock

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools


def main():
    """Run the basic agent example using LangChain 1.x create_agent API."""
    # Initialize DataHub connection
    print("Connecting to DataHub...")
    datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
    if datahub_gms_url is None:
        client = DataHubClient.from_env()
    else:
        client = DataHubClient(
            server=datahub_gms_url, token=os.getenv("DATAHUB_GMS_TOKEN")
        )

    # Create tools using the builder - includes mutation tools for tagging
    tools = build_langchain_tools(client, include_mutations=True)

    # Initialize AWS Bedrock
    aws_region = os.getenv("AWS_REGION", "us-west-2")
    print(f"Connecting to AWS Bedrock in {aws_region}...")

    bedrock_runtime = boto3.client(
        service_name="bedrock-runtime", region_name=aws_region
    )

    model = ChatBedrock(
        client=bedrock_runtime,
        model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
        model_kwargs={"max_tokens": 4096, "temperature": 0},
    )

    # Create agent using LangChain 1.x API
    system_prompt = """You are a helpful data discovery assistant with access to DataHub.

You can help users:
- Find datasets by searching keywords
- Understand data schemas and documentation
- Trace data lineage to see where data comes from and where it goes
- Add metadata like tags to datasets

When searching, always examine the top results to find the most relevant dataset.
When asked about lineage, explain the relationships clearly.
Be concise but informative in your responses."""

    agent = create_agent(model, tools=tools, system_prompt=system_prompt)

    # Example queries
    examples = [
        "Find all datasets related to 'customers'",
        "What are the schemas of the top 2 customer datasets?",
        "Show me the upstream lineage for the first customer dataset you found",
    ]

    print("\n" + "=" * 80)
    print("DataHub LangChain 1.x Agent - Interactive Demo")
    print("=" * 80)

    # Run example queries
    print("\n📋 Example Queries:")
    for i, example in enumerate(examples, 1):
        print(f"{i}. {example}")

    print("\n" + "=" * 80)
    print("Running first example query...\n")

    # Run the first example using LangChain 1.x invoke API
    response = agent.invoke({"messages": [{"role": "user", "content": examples[0]}]})

    # Extract final answer from response
    final_message = response["messages"][-1]
    print("\n" + "=" * 80)
    print(f"Final Answer:\n{final_message.content}")
    print("=" * 80)

    # Interactive mode
    print("\n💬 Interactive Mode - Type 'quit' to exit")
    print("=" * 80)

    while True:
        try:
            user_input = input("\nYou: ").strip()
            if not user_input:
                continue
            if user_input.lower() in ["quit", "exit", "q"]:
                print("Goodbye!")
                break

            response = agent.invoke(
                {"messages": [{"role": "user", "content": user_input}]}
            )
            final_message = response["messages"][-1]
            print(f"\nAgent: {final_message.content}")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()
