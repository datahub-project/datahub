import json
import os
from pathlib import Path
from collections import defaultdict

import mlflow
import pandas as pd
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

# Run name to analyze - update this when you want to analyze a different run
run_name = 'ai_eval_intrigued-croc-408'

print(f"Analyzing tool failures for: {run_name}")

# Get AI eval run
all_runs = mlflow.search_runs(experiment_names=['Chatbot'], max_results=500)
ai_eval_run = all_runs[all_runs['tags.mlflow.runName'] == run_name]

if len(ai_eval_run) == 0:
    raise ValueError(f"Run not found: {run_name}")

print(f"✓ Found run: {run_name}")

# Get run metadata for metrics
run_data = ai_eval_run.iloc[0]
metrics = {k.replace('metrics.', ''): v for k, v in run_data.items() if k.startswith('metrics.') and pd.notna(v)}

# Download and load eval results
results_path = client.download_artifacts(ai_eval_run.iloc[0]['run_id'], 'eval_results_table.json')

with open(results_path) as f:
    results_raw = json.load(f)

results_df = pd.DataFrame(results_raw['data'], columns=results_raw['columns'])

# Set index to prompt_id for easier access
results_df = results_df.set_index('prompt_id')

print(f"Loaded {len(results_df)} prompts")

# Extract tool failure information
# The run_tool/score column contains the overall pass rate for each prompt
# The run_tool/justification column contains a breakdown of tool calls per prompt
# We also look at metrics to get per-tool statistics

# Get tool names from metrics (they follow the pattern run_tool/{tool_name}/pass_percentage)
tool_metrics = {k: v for k, v in metrics.items() if k.startswith('run_tool/') and k.endswith('/pass_percentage')}
tool_names = [k.replace('run_tool/', '').replace('/pass_percentage', '') for k in tool_metrics.keys()]

print(f"Found {len(tool_names)} tools: {tool_names}")

# Collect failures by analyzing the justification column
# Format is like: "Tools: smart_search: 1/1, datahub__get_entities: 0/1"
tool_failures = defaultdict(list)
total_tool_calls = 0
total_tool_failures = 0

for prompt_id in results_df.index:
    justification = results_df.loc[prompt_id, 'run_tool/justification']
    
    # Skip if no tool calls
    if not justification or justification == "No tool calls found":
        continue
    
    # Parse tool breakdown from justification
    # Format: "Tools: tool1: passed/total, tool2: passed/total"
    if justification.startswith("Tools: "):
        tool_breakdown = justification.replace("Tools: ", "")
        tool_parts = tool_breakdown.split(", ")
        
        for tool_part in tool_parts:
            if ": " not in tool_part:
                continue
            tool_name, counts = tool_part.split(": ", 1)
            if "/" not in counts:
                continue
            passed, total = counts.split("/")
            passed, total = int(passed), int(total)
            
            total_tool_calls += total
            failures_count = total - passed
            
            if failures_count > 0:
                total_tool_failures += failures_count
                
                failure_info = {
                    'prompt_id': prompt_id,
                    'message': results_df.loc[prompt_id, 'message'],
                    'tool_name': tool_name,
                    'passed': passed,
                    'total': total,
                    'failures': failures_count,
                }
                
                # Try to get expected tool calls
                try:
                    target = json.loads(results_df.loc[prompt_id, 'target'])
                    expected_tools = target.get('expected_tool_calls', [])
                    failure_info['expected_tools'] = expected_tools
                except:
                    failure_info['expected_tools'] = []
                
                # Try to get actual response
                try:
                    response_msg = json.loads(results_df.loc[prompt_id, 'next_message'])
                    failure_info['response'] = response_msg.get('text', 'N/A')
                except:
                    failure_info['response'] = 'N/A'
                
                # Try to extract error details from chat history
                try:
                    history_json = results_df.loc[prompt_id, 'history']
                    history = json.loads(history_json)
                    messages = history.get('messages', [])
                    
                    # Find ToolResultError messages for this tool
                    error_messages = []
                    for msg in messages:
                        if msg.get('type') == 'tool_result_error':
                            tool_request = msg.get('tool_request', {})
                            if tool_request.get('tool_name') == tool_name:
                                error_messages.append({
                                    'error': msg.get('error', 'Unknown error'),
                                    'tool_params': tool_request.get('tool_params', {})
                                })
                    
                    failure_info['error_details'] = error_messages
                except:
                    failure_info['error_details'] = []
                
                tool_failures[tool_name].append(failure_info)

print(f"\nTotal tool calls: {total_tool_calls}")
print(f"Total tool failures: {total_tool_failures}")
for tool_name, failures in tool_failures.items():
    print(f"  {tool_name}: {len(failures)} prompts with failures")

# Write markdown report
output_path = Path(f'runs/tool_failures_{run_name}.md')
output_path.parent.mkdir(exist_ok=True)

with open(output_path, 'w') as f:
    f.write(f"# Tool Failures Report: {run_name}\n\n")
    f.write(f"**Run:** {run_name}\n")
    f.write(f"**Total Prompts:** {len(results_df)}\n")
    f.write(f"**Total Tool Calls:** {total_tool_calls}\n")
    f.write(f"**Total Tool Failures:** {total_tool_failures}\n\n")
    
    # Summary section
    f.write(f"## Summary\n\n")
    f.write("| Tool | Failures | Success Rate |\n")
    f.write("|------|----------|-------------|\n")
    
    for tool_name in sorted(tool_names):
        failure_count = len(tool_failures.get(tool_name, []))
        success_rate = metrics.get(f'run_tool/{tool_name}/pass_percentage', 0)
        f.write(f"| {tool_name} | {failure_count} | {success_rate:.1f}% |\n")
    
    f.write("\n")
    
    # Overall metrics
    f.write(f"## Overall Metrics\n\n")
    f.write(f"- **Overall tool success rate:** {metrics.get('run_tool/pass_percentage', 0):.1f}%\n")
    f.write(f"- **Guideline adherence:** {metrics.get('guideline_adherence/pass_percentage', 0):.1f}%\n")
    f.write(f"- **Valid links:** {metrics.get('has_valid_links/pass_percentage', 0):.1f}%\n")
    f.write(f"- **Avg response time:** {metrics.get('response_time_avg', 0):.1f}s\n")
    f.write(f"- **Max response time:** {metrics.get('response_time_max', 0):.1f}s\n\n")
    
    # Detailed failures by tool
    for tool_name in sorted(tool_names):
        failures = tool_failures.get(tool_name, [])
        if len(failures) == 0:
            continue
        
        f.write(f"## {tool_name} Failures ({len(failures)} prompts)\n\n")
        
        for failure in failures:
            f.write(f"### {failure['prompt_id']}\n\n")
            f.write(f"**User Message:** {failure['message']}\n\n")
            f.write(f"**Tool Call Results:** {failure['passed']}/{failure['total']} passed ({failure['failures']} failed)\n\n")
            
            if failure['expected_tools']:
                # expected_tools is a list of dicts with 'tool_name' key
                expected_names = [
                    tool['tool_name'] if isinstance(tool, dict) else str(tool) 
                    for tool in failure['expected_tools']
                ]
                f.write(f"**Expected Tool Calls:** {', '.join(expected_names)}\n\n")
            
            # Show error details if available
            if failure.get('error_details'):
                f.write(f"**Error Details:**\n")
                for idx, error in enumerate(failure['error_details'], 1):
                    f.write(f"\n**Error {idx}:**\n```\n{error['error']}\n```\n")
                    if error.get('tool_params'):
                        # Show a truncated version of tool params
                        params_str = str(error['tool_params'])
                        if len(params_str) > 300:
                            params_str = params_str[:300] + "...[truncated]"
                        f.write(f"\n**Tool Parameters:**\n```json\n{params_str}\n```\n")
                f.write("\n")
            
            # Show response excerpt if available
            response = failure['response']
            if response and response != 'N/A':
                # Truncate long responses
                if len(response) > 500:
                    response = response[:500] + "...\n[truncated]"
                f.write(f"**Response (excerpt):**\n```\n{response}\n```\n\n")
            
            f.write("---\n\n")
    
    # Prompts with no tool failures but other issues
    f.write(f"## Additional Analysis\n\n")
    
    # Find prompts with guideline failures but no tool failures
    guideline_failures = results_df[results_df['guideline_adherence/score'] == False]
    tool_success = results_df[results_df['run_tool/score'] == True]
    guideline_only_failures = guideline_failures.index.intersection(tool_success.index)
    
    if len(guideline_only_failures) > 0:
        f.write(f"### Guideline Failures (Tools Succeeded) - {len(guideline_only_failures)}\n\n")
        f.write("These prompts successfully called the expected tools but failed guideline adherence:\n\n")
        for prompt_id in guideline_only_failures[:5]:  # Show first 5
            f.write(f"- **{prompt_id}**: {results_df.loc[prompt_id, 'message'][:80]}...\n")
        if len(guideline_only_failures) > 5:
            f.write(f"\n...and {len(guideline_only_failures) - 5} more\n")
        f.write("\n")
    
    # Find prompts with link validation failures
    link_failures = results_df[results_df['has_valid_links/score'] == False]
    if len(link_failures) > 0:
        f.write(f"### Invalid Links - {len(link_failures)}\n\n")
        f.write("Prompts with invalid or missing DataHub entity links:\n\n")
        for prompt_id in link_failures.index[:5]:
            justification = results_df.loc[prompt_id, 'has_valid_links/justification']
            f.write(f"- **{prompt_id}**: {justification}\n")
        if len(link_failures) > 5:
            f.write(f"\n...and {len(link_failures) - 5} more\n")
        f.write("\n")

print(f"\n✓ Report written to: {output_path}")
print(f"Total size: {output_path.stat().st_size / 1024:.1f} KB")

