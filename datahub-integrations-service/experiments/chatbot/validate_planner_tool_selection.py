#!/usr/bin/env python3
"""
Validate Planner Tool Selection

Tests that the planner LLM chooses the correct planning tool
(create_noop_plan, create_templated_plan, or create_execution_plan)
for different query types.

Based on analysis of 321 real customer queries.

Usage:
    cd experiments/chatbot
    python validate_planner_tool_selection.py
"""

import os
import pathlib
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()

from loguru import logger

# Setup logging
_LOG_DIR = pathlib.Path(__file__).parent / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
_LOG_FILE = _LOG_DIR / f"planner_validation_{_TIMESTAMP}.log"

logger.remove()

# File logger: DEBUG level for everything (detailed logs)
logger.add(
    _LOG_FILE,
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
)

# Console logger: Only INFO from this validation script (clean output)
def console_filter(record):
    """Only show INFO+ messages from the validation script itself."""
    return record["name"] == "__main__"

logger.add(
    sys.stdout,
    level="INFO",
    format="{message}",
    filter=console_filter,
)

print(f"Planner validation logging to: {_LOG_FILE.name}\n")

from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED
from datahub.sdk.main_client import DataHubClient
from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
from datahub_integrations.mcp.mcp_server import set_datahub_client

assert AI_EXPERIMENTATION_INITIALIZED


# =============================================================================
# Test Queries (Obfuscated from Real Customer Data)
# =============================================================================

NOOP_QUERIES = [
    "show me the schema of the customers table",
    "list all glossary terms",
    "who owns the analytics warehouse",
    "can I have the link to the sales dashboard",
    "how many columns are in the users table",
    "what tables are in PROD_DB.ANALYTICS",
    "show me the schema for orders dataset",
    "what columns are in the transactions table",
]

DEFINITION_DISCOVERY_QUERIES = [
    "what is MAU",
    "what is the definition of MAU metric",
    "what is the definition of revenue",
    "what does market_type mean in the organization table",
    "what is add to cart rate",
    "what is turnover",
    "what is GGR",
    "what is average cart size",
    "what does EXPERIMENT_GROUP field mean",
]

DATA_LOCATION_QUERIES = [
    "where can I find churn data",
    "where can I find monthly churn rate by region",
    "where do I find inventory aging data",
    "where could I find web signups for small business customers",
    "where can I find click and collect data",
    "where do I find delivery metrics",
]

JOIN_DISCOVERY_QUERIES = [
    "how do I join accounts table with organizations",
    "how can I join events with users",
    "how do I establish relationship between partner_id and org_id",
]

IMPACT_ANALYSIS_QUERIES = [
    "if I delete users table what breaks",
    "if I change customer_profiles what are the downstream impacts",
    "what will be impacted if I deprecate the analytics model",
    "if I make a change to dim_user what assets will be impacted and who owns them",
]

EXECUTION_QUERIES = [
    "which table contains organization daily subscription with columns org_id and plan_type",
    "how can I identify which customers upgraded subscription tier by region",
    "are there any dashboards showing partner health metrics",
    "what's the fundamental difference between fct_org_daily, fct_org_monthly, and fct_org_current",
    "find all datasets with PII and show their owners and usage",
    "how many customers have been with us for over 5 years",
    "what are the 10 most used dashboards built on the sales explore",
]

# =============================================================================
# Production Test Cases (from prompts.yaml)
# =============================================================================

PROD_NOOP_QUERIES = [
    "what's the schema of long_tail_companions.analytics.pet_details table?",
]

PROD_JOIN_DISCOVERY_QUERIES = [
    "what other tables are typically joined with pet profiles",
    "what other tables are typically joined with pet profiles? Provide column names and semantic understanding of the join.",
    "what other tables are typically joined with contact_list table based on query logs?",
    "what other tables are typically joined with STG_HUBSPOT__CONTACT_LIST table?",
]

PROD_DATA_LOCATION_QUERIES = [
    "I need to find out list of organizations with their contact information to send out a promotion email. Which table is better between datavault_hway.data_vault.sat_salesforceaccount_sfdc_account and xade_hway_curated_nrt.salesforce_sfdc.contact?",
]

PROD_IMPACT_ANALYSIS_QUERIES = [
    "I'm planning on changing the `color` column in the `dbt: long_tail_companions.analytics.pet_details` table. What stuff will be impacted by this?",
    "What is the impact of change in organisation_name column in CORE_XERO_MODELS_HWAY.INFO_MART.DIM_ORGANISATION_CURRENT dbt model?",
    "can you tell me what users will be impacted if I delete the org_analyser model?",
    "which Tableau dashboards would be affected if I dropped the probability column from the opportunity table in source_ops_sfdc_production_current?",
    "How many assets will be directly and indirectly impacted if I remove the transaction_type column of kafka account_transactions dataset?",
    "What would be impacted if I remove the account_transactions dataset?",
    "What would be directly impacted if I remove the kafka account_transactions dataset?",
]

PROD_EXECUTION_QUERIES = [
    "what data do we have related to pets?",
    "write an SQL query to get pet profile creations by month using the dbt pet_profiles model",
    "write an SQL query to get pet profile creations by month",
    "How do I find out what organizations have created an invoice?",
    "How do I find which organizations are currently on premium pricing plan?",
    "I would like to find out which organizations are currently using point of sale app.",
    "Which tables can I use to find out list of organizations with their contact information to send out a promotion email?",
    "Show me largest unused tables in snowflake.",
    "Which datasets are marked as deprecated in Sales domain?",
    "Recommend data sources for churn prediction.",
    "Generate an SQL query to find how many active non-churned customers have a customer tier attached to them.",
    "Generate an SQL query to count how many distinct aspects we have in our metadata.",
]


# =============================================================================
# Validation Logic
# =============================================================================


def extract_tool_used(agent, plan) -> str:
    """
    Extract which internal tool was used to create the plan.
    
    Reads from plan_cache["internal"]["tool_used"] which is set by create_plan.
    Falls back to plan structure detection if cache data not available.
    
    Args:
        agent: AgentRunner instance
        plan: The Plan object returned by create_plan
        
    Returns:
        Tool name: "create_noop_plan", "create_templated_plan[template_id]", or "create_execution_plan"
    """
    # Try to get from cache first (most reliable)
    cache_entry = agent.plan_cache.get(plan.plan_id)
    if cache_entry and "internal" in cache_entry:
        internal = cache_entry["internal"]
        tool_used = internal.get("tool_used", "unknown")
        template_id = internal.get("template_id")
        
        if tool_used == "create_templated_plan" and template_id:
            return f"create_templated_plan[{template_id}]"
        return tool_used
    
    # Fallback: detect from plan structure
    if plan.is_noop:
        return "create_noop_plan"
    else:
        return "create_execution_plan"


def validate_query(agent, query: str, expected_tool: str) -> dict:
    """
    Validate that the planner chooses the expected tool for a query.
    
    Args:
        agent: AgentRunner instance
        query: The test query
        expected_tool: Expected tool name (or prefix like "create_templated_plan")
        
    Returns:
        dict with query, expected, actual, passed
    """
    from datahub_integrations.chat.planner.tools import create_plan
    
    logger.debug(f"\n{'='*60}")
    logger.debug(f"Testing query: {query}")
    logger.debug(f"Expected: {expected_tool}")
    
    try:
        plan = create_plan(agent=agent, task=query)
        actual_tool = extract_tool_used(agent, plan)
        
        # Check if it matches (for templates, just check prefix)
        if expected_tool.startswith("create_templated_plan"):
            passed = actual_tool.startswith("create_templated_plan")
        else:
            passed = actual_tool == expected_tool
        
        return {
            "query": query,
            "expected": expected_tool,
            "actual": actual_tool,
            "passed": passed,
            "plan_id": plan.plan_id,
        }
    except Exception as e:
        logger.debug(f"Exception during validation: {e}", exc_info=True)
        return {
            "query": query,
            "expected": expected_tool,
            "actual": f"ERROR: {e}",
            "passed": False,
            "plan_id": None,
        }


def run_validation(agent):
    """Run validation for all query categories."""
    results = []
    
    print("\n" + "="*80)
    print("NOOP QUERIES (expect create_noop_plan)")
    print("="*80)
    for query in NOOP_QUERIES:
        result = validate_query(agent, query, "create_noop_plan")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\n" + "="*80)
    print("DEFINITION DISCOVERY (expect create_templated_plan[template-definition-discovery])")
    print("="*80)
    for query in DEFINITION_DISCOVERY_QUERIES:
        result = validate_query(agent, query, "create_templated_plan[template-definition-discovery]")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\n" + "="*80)
    print("DATA LOCATION (expect create_templated_plan[template-data-location])")
    print("="*80)
    for query in DATA_LOCATION_QUERIES:
        result = validate_query(agent, query, "create_templated_plan[template-data-location]")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\n" + "="*80)
    print("JOIN DISCOVERY (expect create_templated_plan[template-join-discovery])")
    print("="*80)
    for query in JOIN_DISCOVERY_QUERIES:
        result = validate_query(agent, query, "create_templated_plan[template-join-discovery]")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\n" + "="*80)
    print("IMPACT ANALYSIS (expect create_templated_plan[template-impact-analysis])")
    print("="*80)
    for query in IMPACT_ANALYSIS_QUERIES:
        result = validate_query(agent, query, "create_templated_plan[template-impact-analysis]")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\n" + "="*80)
    print("EXECUTION (expect create_execution_plan)")
    print("="*80)
    for query in EXECUTION_QUERIES:
        result = validate_query(agent, query, "create_execution_plan")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    # =============================================================================
    # Production Test Cases (from prompts.yaml)
    # =============================================================================
    
    print("\n" + "="*80)
    print("PRODUCTION TEST CASES (from prompts.yaml)")
    print("="*80)
    
    print("\nPROD NOOP:")
    for query in PROD_NOOP_QUERIES:
        result = validate_query(agent, query, "create_noop_plan")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\nPROD JOIN DISCOVERY:")
    for query in PROD_JOIN_DISCOVERY_QUERIES:
        result = validate_query(agent, query, "create_templated_plan[template-join-discovery]")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\nPROD DATA LOCATION:")
    for query in PROD_DATA_LOCATION_QUERIES:
        result = validate_query(agent, query, "create_templated_plan[template-data-location]")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\nPROD IMPACT ANALYSIS:")
    for query in PROD_IMPACT_ANALYSIS_QUERIES:
        result = validate_query(agent, query, "create_templated_plan[template-impact-analysis]")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    print("\nPROD EXECUTION:")
    for query in PROD_EXECUTION_QUERIES:
        result = validate_query(agent, query, "create_execution_plan")
        results.append(result)
        status = "✓" if result["passed"] else "✗"
        print(f"{status} {query[:60]:<60} → {result['actual']}")
    
    return results


def print_summary(results):
    """Print summary statistics."""
    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total: {total}")
    print(f"Passed: {passed} ({passed/total*100:.1f}%)")
    print(f"Failed: {failed} ({failed/total*100:.1f}%)")
    
    # Per-category breakdown
    categories = {
        "NOOP (synthetic)": "create_noop_plan",
        "DEFINITION DISCOVERY (synthetic)": "create_templated_plan[template-definition-discovery]",
        "DATA LOCATION (synthetic)": "create_templated_plan[template-data-location]",
        "JOIN DISCOVERY (synthetic)": "create_templated_plan[template-join-discovery]",
        "IMPACT ANALYSIS (synthetic)": "create_templated_plan[template-impact-analysis]",
        "EXECUTION (synthetic)": "create_execution_plan",
    }
    
    # Count production test cases separately
    prod_noop_count = len(PROD_NOOP_QUERIES)
    prod_join_count = len(PROD_JOIN_DISCOVERY_QUERIES)
    prod_data_location_count = len(PROD_DATA_LOCATION_QUERIES)
    prod_impact_count = len(PROD_IMPACT_ANALYSIS_QUERIES)
    prod_exec_count = len(PROD_EXECUTION_QUERIES)
    prod_total = prod_noop_count + prod_join_count + prod_data_location_count + prod_impact_count + prod_exec_count
    
    synthetic_total = len(NOOP_QUERIES) + len(DEFINITION_DISCOVERY_QUERIES) + len(DATA_LOCATION_QUERIES) + len(JOIN_DISCOVERY_QUERIES) + len(IMPACT_ANALYSIS_QUERIES) + len(EXECUTION_QUERIES)
    
    print(f"\nTest Set Breakdown:")
    print(f"  Synthetic queries: {synthetic_total}")
    print(f"  Production queries (prompts.yaml): {prod_total}")
    
    print("\nPer-Category Results:")
    for cat_name, expected_prefix in categories.items():
        cat_results = [r for r in results if r["expected"].startswith(expected_prefix)]
        if cat_results:
            cat_passed = sum(1 for r in cat_results if r["passed"])
            cat_total = len(cat_results)
            print(f"  {cat_name}: {cat_passed}/{cat_total} ({cat_passed/cat_total*100:.0f}%)")
    
    # Production categories
    if prod_total > 0:
        print("\nProduction Test Cases (prompts.yaml):")
        prod_cats = {
            "NOOP": ("create_noop_plan", prod_noop_count),
            "JOIN DISCOVERY": ("create_templated_plan[template-join-discovery]", prod_join_count),
            "DATA LOCATION": ("create_templated_plan[template-data-location]", prod_data_location_count),
            "IMPACT ANALYSIS": ("create_templated_plan[template-impact-analysis]", prod_impact_count),
            "EXECUTION": ("create_execution_plan", prod_exec_count),
        }
        for cat_name, (expected_prefix, expected_count) in prod_cats.items():
            if expected_count > 0:
                cat_results = [r for r in results if r["expected"].startswith(expected_prefix) and r["query"] in (PROD_NOOP_QUERIES + PROD_JOIN_DISCOVERY_QUERIES + PROD_DATA_LOCATION_QUERIES + PROD_IMPACT_ANALYSIS_QUERIES + PROD_EXECUTION_QUERIES)]
                cat_passed = sum(1 for r in cat_results if r["passed"])
                cat_total = len(cat_results)
                if cat_total > 0:
                    print(f"  {cat_name}: {cat_passed}/{cat_total} ({cat_passed/cat_total*100:.0f}%)")
    
    # Show failures
    failures = [r for r in results if not r["passed"]]
    if failures:
        print("\nFailed Cases:")
        for f in failures:
            print(f"  ✗ {f['query'][:50]:<50} | Expected: {f['expected']:<40} | Got: {f['actual']}")


# =============================================================================
# Main
# =============================================================================


def main():
    """Main validation entry point."""
    logger.info("Initializing DataHub client...")
    client = DataHubClient.from_env()
    
    # Set the client in ContextVar so planner tools can access it
    set_datahub_client(client)
    
    logger.info("Creating agent...")
    agent = create_data_catalog_explorer_agent(client=client)
    
    logger.info("\nStarting validation...\n")
    results = run_validation(agent)
    
    print_summary(results)
    
    logger.info(f"\nFull logs written to: {_LOG_FILE}")


if __name__ == "__main__":
    main()

