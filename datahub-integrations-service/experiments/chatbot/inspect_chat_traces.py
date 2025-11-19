"""
Inspect chat_ui traces from MLflow to debug LLM responses.

This script retrieves the latest chat traces from MLflow and displays:
- Message content (to see if reasoning is present)
- Tool calls
- Response structure
- Token usage
"""

import json
import os
from pathlib import Path

import mlflow
from dotenv import load_dotenv

# Load .env configuration
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Set up MLflow tracking URI from environment
tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:9090")
print(f"Using MLflow tracking URI: {tracking_uri}")

# Import sagemaker plugin if using ARN
if tracking_uri.startswith("arn:aws:sagemaker"):
    import sagemaker_mlflow

    print(f"Using AWS profile: {os.getenv('AWS_PROFILE', 'default')}")

mlflow.set_tracking_uri(tracking_uri)
client = mlflow.MlflowClient()

# Search for recent chat runs (not eval runs)
print()
print("Searching for recent chat sessions...")
print()

# Get runs from Chatbot experiment
all_runs = mlflow.search_runs(
    experiment_names=["Chatbot"], max_results=50, order_by=["start_time DESC"]
)

# Filter out AI eval runs - we want actual chat sessions
chat_runs = all_runs[~all_runs["tags.mlflow.runName"].str.startswith("ai_eval", na=False)]

if chat_runs.empty:
    print("No chat runs found (only eval runs)")
    exit(0)

print(f"Found {len(chat_runs)} recent chat sessions (excluding eval runs)")
print()

# Show the most recent ones
for idx in range(min(5, len(chat_runs))):
    run = chat_runs.iloc[idx]
    run_id = run["run_id"]
    start_time = run["start_time"]
    status = run["status"]

    # Extract session_id tag if available
    session_id = run.get("tags.session_id", "unknown")

    print(f"{idx + 1}. Run: {run_id}")
    print(f"   Time: {start_time}")
    print(f"   Status: {status}")
    print(f"   Session ID: {session_id}")

    # Try to get trace data
    try:
        trace = mlflow.get_trace(run_id)
        if trace:
            print(f"   ✅ Trace available with {len(trace.data.spans)} spans")

            # Look for key spans
            span_names = [span.name for span in trace.data.spans]
            has_reasoning = any("reasoning" in name.lower() for name in span_names)
            has_tool_call = any("tool" in name.lower() for name in span_names)

            print(f"   Has reasoning spans: {has_reasoning}")
            print(f"   Has tool call spans: {has_tool_call}")

            # Get root span (generate_next_message)
            root_span = next(
                (s for s in trace.data.spans if "generate_next_message" in s.name), None
            )
            if root_span and root_span.outputs:
                output_str = str(root_span.outputs)[:200]
                print(f"   Output preview: {output_str}...")
        else:
            print(f"   ⚠️  Trace not accessible")
    except Exception as e:
        print(f"   ⚠️  Error accessing trace: {e}")

    print()

# Ask user which trace to inspect in detail
print("=" * 70)
print("Select a run to inspect in detail (1-5), or press Enter to inspect #1:")
selection = input().strip() or "1"

try:
    selected_idx = int(selection) - 1
    if selected_idx < 0 or selected_idx >= len(chat_runs):
        print("Invalid selection")
        exit(1)
except ValueError:
    print("Invalid selection")
    exit(1)

selected_run = chat_runs.iloc[selected_idx]
run_id = selected_run["run_id"]

print()
print("=" * 70)
print(f"Inspecting Run: {run_id}")
print("=" * 70)
print()

# Get the trace
try:
    trace = mlflow.get_trace(run_id)
    if not trace:
        print("Trace not available")
        exit(1)

    print(f"Trace has {len(trace.data.spans)} spans")
    print()

    # Display all spans
    print("Span Hierarchy:")
    print("-" * 70)
    for i, span in enumerate(trace.data.spans):
        indent = "  " * (span.name.count("_") - 1)  # Simple depth heuristic
        print(f"{i:2d}. {indent}{span.name} ({span.span_type})")

    print()
    print("=" * 70)
    print("Detailed Span Analysis")
    print("=" * 70)

    # Analyze reasoning spans
    reasoning_spans = [s for s in trace.data.spans if "reasoning" in s.name.lower()]
    if reasoning_spans:
        print(f"\n📝 Found {len(reasoning_spans)} reasoning spans:")
        for span in reasoning_spans:
            print(f"\n  Span: {span.name}")
            if span.outputs:
                print(f"  Reasoning content:")
                reasoning_text = span.outputs.get("reasoning", "N/A")
                print(f"    {reasoning_text[:500]}...")
    else:
        print("\n⚠️  No reasoning spans found")

    # Analyze tool call spans
    tool_spans = [
        s for s in trace.data.spans if "tool" in s.name.lower() and "call" in s.name.lower()
    ]
    if tool_spans:
        print(f"\n🔧 Found {len(tool_spans)} tool call spans:")
        for span in tool_spans[:3]:  # Show first 3
            print(f"\n  Span: {span.name}")
            if span.inputs:
                print(f"  Tool: {span.inputs.get('tool_name', 'N/A')}")
            if span.outputs:
                result_str = str(span.outputs)[:300]
                print(f"  Result preview: {result_str}...")

    # Look for LLM call spans with response details
    print()
    print("=" * 70)
    print("LLM Response Analysis")
    print("=" * 70)

    # Find spans that might have the raw LLM response
    llm_spans = [
        s
        for s in trace.data.spans
        if s.span_type == "LLM" or "generate_tool_call" in s.name.lower()
    ]

    for span in llm_spans[:3]:  # Show first 3 LLM calls
        print(f"\nSpan: {span.name}")
        print(f"Type: {span.span_type}")

        if span.outputs:
            outputs = span.outputs
            print(f"\nOutputs keys: {list(outputs.keys())}")

            # Look for response or message
            if "response" in outputs:
                response = outputs["response"]
                print(f"\nResponse keys: {list(response.keys()) if isinstance(response, dict) else 'N/A'}")

                if isinstance(response, dict):
                    # Check for content
                    if "content" in response:
                        print(f"  content: {response['content']}")
                    if "tool_calls" in response:
                        print(f"  tool_calls: {response['tool_calls']}")
                    if "output" in response:
                        output = response["output"]
                        if isinstance(output, dict) and "message" in output:
                            msg_content = output["message"].get("content", [])
                            print(f"\n  Message content blocks ({len(msg_content)}):")
                            for j, block in enumerate(msg_content[:5]):
                                if isinstance(block, dict):
                                    block_type = (
                                        "text"
                                        if "text" in block
                                        else "toolUse"
                                        if "toolUse" in block
                                        else list(block.keys())[0]
                                    )
                                    print(f"    {j}. {block_type}")
                                    if "text" in block:
                                        text_preview = block["text"][:200]
                                        print(f"       Content: {text_preview}...")
                                    elif "toolUse" in block:
                                        tool_name = block["toolUse"].get("name", "N/A")
                                        print(f"       Tool: {tool_name}")

        if span.attributes:
            attrs = span.attributes
            print(f"\nAttributes:")
            if "reasoning_length" in attrs:
                print(f"  reasoning_length: {attrs['reasoning_length']}")
            if "response_length" in attrs:
                print(f"  response_length: {attrs['response_length']}")

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print()
    print(
        "If content is null/empty and only tool_calls are present, this is EXPECTED"
    )
    print("behavior for OpenAI models. They're trained to be direct and efficient.")
    print()
    print("Claude generates reasoning text, OpenAI often skips it for tool calls.")
    print("Both approaches work correctly - just different model behaviors.")

except Exception as e:
    print(f"Error inspecting trace: {e}")
    import traceback

    traceback.print_exc()

