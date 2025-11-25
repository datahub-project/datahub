import json
import os
from pathlib import Path

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

# Run names are hardcoded here (intentional - helps remember which baseline we're using)
# Update these when you want to compare different runs
current_run_name = 'ai_eval_intrigued-croc-408'
baseline_run_name = 'ai_eval_wise-hare-663'  # Baseline copied from local server

print(f"Comparing: {current_run_name} (current) vs {baseline_run_name} (baseline)")

# Get AI eval runs
all_runs = mlflow.search_runs(experiment_names=['Chatbot'], max_results=500)
ai_eval_current = all_runs[all_runs['tags.mlflow.runName'] == current_run_name]
ai_eval_baseline = all_runs[all_runs['tags.mlflow.runName'] == baseline_run_name]

if len(ai_eval_current) == 0:
    raise ValueError(f"Current run not found: {current_run_name}")
if len(ai_eval_baseline) == 0:
    raise ValueError(f"Baseline run not found: {baseline_run_name}")

print(f"✓ Found current run: {current_run_name}")
print(f"✓ Found baseline run: {baseline_run_name}")

# Get run metadata for metrics
current_run = ai_eval_current.iloc[0]
baseline_run = ai_eval_baseline.iloc[0]
metrics_current = {k.replace('metrics.', ''): v for k, v in current_run.items() if k.startswith('metrics.') and pd.notna(v)}
metrics_baseline = {k.replace('metrics.', ''): v for k, v in baseline_run.items() if k.startswith('metrics.') and pd.notna(v)}

# Download and load eval results
current_path = client.download_artifacts(ai_eval_current.iloc[0]['run_id'], 'eval_results_table.json')
baseline_path = client.download_artifacts(ai_eval_baseline.iloc[0]['run_id'], 'eval_results_table.json')

with open(current_path) as f:
    current_raw = json.load(f)
with open(baseline_path) as f:
    baseline_raw = json.load(f)

current_df = pd.DataFrame(current_raw['data'], columns=current_raw['columns'])
baseline_df = pd.DataFrame(baseline_raw['data'], columns=baseline_raw['columns'])

# Set index to prompt_id for easier comparison
current_df = current_df.set_index('prompt_id')
baseline_df = baseline_df.set_index('prompt_id')

# Check score values
score_col = 'guideline_adherence/score'
print(f"Sample current scores: {current_df[score_col].head()}")
print(f"Sample baseline scores: {baseline_df[score_col].head()}")
print(f"Current score dtype: {current_df[score_col].dtype}")
print(f"Baseline score dtype: {baseline_df[score_col].dtype}")

# Convert boolean to int if needed
if current_df[score_col].dtype == bool:
    current_df['score'] = current_df[score_col].astype(int)
else:
    current_df['score'] = pd.to_numeric(current_df[score_col], errors='coerce')
    
if baseline_df[score_col].dtype == bool:
    baseline_df['score'] = baseline_df[score_col].astype(int)
else:
    baseline_df['score'] = pd.to_numeric(baseline_df[score_col], errors='coerce')

# Compare scores
comparison = pd.DataFrame({
    'current_score': current_df['score'],
    'baseline_score': baseline_df['score']
})

# Find changes - calculate after ensuring numeric
comparison['score_diff'] = comparison['current_score'].astype(float) - comparison['baseline_score'].astype(float)

# Find changes
new_failures = comparison[(comparison['current_score'] < 1.0) & (comparison['baseline_score'] >= 1.0)]
fixed = comparison[(comparison['current_score'] >= 1.0) & (comparison['baseline_score'] < 1.0)]
degraded = comparison[comparison['score_diff'] < -0.05]  # Score got worse
improved = comparison[comparison['score_diff'] > 0.05]   # Score got better

print(f"\nNew failures (passed -> failed): {len(new_failures)}")
print(f"Fixed (failed -> passed): {len(fixed)}")
print(f"Degraded score: {len(degraded)}")
print(f"Improved score: {len(improved)}")

if len(new_failures) > 0:
    print(f"\nNew failure prompt IDs: {list(new_failures.index[:5])}")
if len(fixed) > 0:
    print(f"Fixed prompt IDs: {list(fixed.index[:5])}")

# Write markdown report
output_path = Path(f'runs/eval_comparison_{current_run_name}_vs_{baseline_run_name}.md')
output_path.parent.mkdir(exist_ok=True)

with open(output_path, 'w') as f:
    f.write(f"# Evaluation Comparison: {current_run_name} vs {baseline_run_name}\n\n")
    f.write(f"**Current Run:** {current_run_name}\n")
    f.write(f"**Baseline Run:** {baseline_run_name}\n\n")
    
    f.write(f"## Summary\n\n")
    f.write(f"- **New failures:** {len(new_failures)} prompts (passed in baseline → failed in current)\n")
    f.write(f"- **Fixed:** {len(fixed)} prompts (failed in baseline → passed in current)\n")
    f.write(f"- **Degraded score:** {len(degraded)} prompts\n")
    f.write(f"- **Improved score:** {len(improved)} prompts\n\n")
    
    # Add metrics comparison table
    f.write(f"## Metrics Comparison\n\n")
    f.write("| Metric | Current | Baseline | Change |\n")
    f.write("|--------|---------|----------|--------|\n")
    
    # Key metrics
    guideline_curr = metrics_current.get('guideline_adherence/pass_percentage', 0)
    guideline_base = metrics_baseline.get('guideline_adherence/pass_percentage', 0)
    guideline_diff = guideline_curr - guideline_base
    f.write(f"| **Guideline adherence** | {guideline_curr:.1f}% | {guideline_base:.1f}% | {guideline_diff:+.1f}% |\n")
    
    overall_curr = metrics_current.get('run_tool/pass_percentage', 0)
    overall_base = metrics_baseline.get('run_tool/pass_percentage', 0)
    overall_diff = overall_curr - overall_base
    f.write(f"| **Overall tool success** | {overall_curr:.1f}% | {overall_base:.1f}% | {overall_diff:+.1f}% |\n")
    
    smart_curr = metrics_current.get('run_tool/smart_search/pass_percentage', 0)
    smart_base = metrics_baseline.get('run_tool/smart_search/pass_percentage', 0)
    smart_diff = smart_curr - smart_base
    f.write(f"| Smart search | {smart_curr:.1f}% | {smart_base:.1f}% | {smart_diff:+.1f}% |\n")
    
    lineage_curr = metrics_current.get('run_tool/datahub__get_lineage/pass_percentage', 0)
    lineage_base = metrics_baseline.get('run_tool/datahub__get_lineage/pass_percentage', 0)
    lineage_diff = lineage_curr - lineage_base
    f.write(f"| Get lineage | {lineage_curr:.1f}% | {lineage_base:.1f}% | {lineage_diff:+.1f}% |\n")
    
    entities_curr = metrics_current.get('run_tool/datahub__get_entities/pass_percentage', 0)
    entities_base = metrics_baseline.get('run_tool/datahub__get_entities/pass_percentage', 0)
    entities_diff = entities_curr - entities_base
    f.write(f"| Get entities | {entities_curr:.1f}% | {entities_base:.1f}% | {entities_diff:+.1f}% |\n")
    
    search_curr = metrics_current.get('run_tool/datahub__search/pass_percentage', 0)
    search_base = metrics_baseline.get('run_tool/datahub__search/pass_percentage', 0)
    search_diff = search_curr - search_base
    f.write(f"| Search | {search_curr:.1f}% | {search_base:.1f}% | {search_diff:+.1f}% |\n")
    
    links_curr = metrics_current.get('has_valid_links/pass_percentage', 0)
    links_base = metrics_baseline.get('has_valid_links/pass_percentage', 0)
    links_diff = links_curr - links_base
    f.write(f"| **Valid links** | {links_curr:.1f}% | {links_base:.1f}% | {links_diff:+.1f}% |\n")
    
    time_curr = metrics_current.get('response_time_avg', 0)
    time_base = metrics_baseline.get('response_time_avg', 0)
    time_diff = time_curr - time_base
    f.write(f"| Avg response time | {time_curr:.1f}s | {time_base:.1f}s | {time_diff:+.1f}s |\n")
    
    max_time_curr = metrics_current.get('response_time_max', 0)
    max_time_base = metrics_baseline.get('response_time_max', 0)
    max_time_diff = max_time_curr - max_time_base
    f.write(f"| Max response time | {max_time_curr:.1f}s | {max_time_base:.1f}s | {max_time_diff:+.1f}s |\n")
    
    f.write("\n")
    
    # New failures section
    if len(new_failures) > 0:
        f.write(f"## New Failures ({len(new_failures)})\n\n")
        f.write("Prompts that passed the baseline but failed in the current run.\n\n")
        
        for prompt_id in new_failures.index:
            f.write(f"### {prompt_id}\n\n")
            
            # Get details
            message = current_df.loc[prompt_id, 'message']
            guidelines = current_df.loc[prompt_id, 'response_guidelines']
            current_just = current_df.loc[prompt_id, 'guideline_adherence/justification']
            baseline_just = baseline_df.loc[prompt_id, 'guideline_adherence/justification']
            
            # Get responses
            try:
                current_msg = json.loads(current_df.loc[prompt_id, 'next_message'])
                current_response = current_msg.get('text', 'N/A')
            except:
                current_response = "N/A"
                
            try:
                baseline_msg = json.loads(baseline_df.loc[prompt_id, 'next_message'])
                baseline_response = baseline_msg.get('text', 'N/A')
            except:
                baseline_response = "N/A"
            
            f.write(f"**User Message:** {message}\n\n")
            f.write(f"**Guidelines:** {guidelines}\n\n")
            f.write(f"**Scores:** Baseline={new_failures.loc[prompt_id, 'baseline_score']}, Current={new_failures.loc[prompt_id, 'current_score']}\n\n")
            f.write(f"**Current Response:**\n```\n{current_response}\n```\n\n")
            f.write(f"**Baseline Response:**\n```\n{baseline_response}\n```\n\n")
            f.write(f"**Current Judge Comment:**\n```\n{current_just}\n```\n\n")
            f.write(f"**Baseline Judge Comment:**\n```\n{baseline_just}\n```\n\n")
            f.write("---\n\n")
    
    # Degraded section
    if len(degraded) > 0:
        f.write(f"\n## Degraded Scores ({len(degraded)})\n\n")
        f.write("Prompts where the score got worse (even if still passing/failing).\n\n")
        
        for prompt_id in degraded.index[:10]:  # Limit to first 10
            f.write(f"### {prompt_id}\n\n")
            
            message = current_df.loc[prompt_id, 'message']
            guidelines = current_df.loc[prompt_id, 'response_guidelines']
            current_just = current_df.loc[prompt_id, 'guideline_adherence/justification']
            baseline_just = baseline_df.loc[prompt_id, 'guideline_adherence/justification']
            
            f.write(f"**User Message:** {message}\n\n")
            f.write(f"**Guidelines:** {guidelines}\n\n")
            f.write(f"**Score Change:** {degraded.loc[prompt_id, 'baseline_score']} → {degraded.loc[prompt_id, 'current_score']} ({degraded.loc[prompt_id, 'score_diff']:.2f})\n\n")
            f.write(f"**Current Judge Comment:**\n```\n{current_just}\n```\n\n")
            f.write(f"**Baseline Judge Comment:**\n```\n{baseline_just}\n```\n\n")
            f.write("---\n\n")

print(f"\n✓ Report written to: {output_path}")
print(f"Total size: {output_path.stat().st_size / 1024:.1f} KB")

