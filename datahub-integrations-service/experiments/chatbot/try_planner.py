"""
Simple experiment script to invoke and validate the planning tools.

This is NOT a pytest test - it's a manual validation script.

Usage:
    cd datahub-integrations-service/experiments/chatbot
    python try_planner.py
"""

import sys
from pathlib import Path

# Add src to path to import our modules
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from datahub_integrations.chat.planner.tools import (
    create_plan,
    report_step_progress,
    revise_plan,
)


def try_create_plan():
    """Try creating a plan for a complex task."""
    print("\n" + "=" * 80)
    print("EXPERIMENT 1: Creating a plan for impact analysis")
    print("=" * 80)
    
    task = "What Looker dashboards would be affected if we deprecate the orders dataset?"
    context = "This is a production environment analysis"
    
    print(f"\nTask: {task}")
    print(f"Context: {context}")
    print("\nCalling create_plan...")
    
    try:
        plan = create_plan(
            task=task,
            context=context,
            max_steps=7,
            time_budget_seconds=60,
        )
        
        print(f"\n✅ Plan created successfully!")
        print(f"   Plan ID: {plan.plan_id}")
        print(f"   Version: {plan.version}")
        print(f"   Title: {plan.title}")
        print(f"   Goal: {plan.goal}")
        print(f"   Number of steps: {len(plan.steps)}")
        print(f"   Tool allowlist: {len(plan.constraints.tool_allowlist)} tools")
        
        print(f"\n📋 Steps:")
        for step in plan.steps:
            print(f"   {step.id}: {step.description}")
            if step.done_when:
                print(f"      Done when: {step.done_when}")
            if step.tool:
                print(f"      Tool: {step.tool}")
        
        print(f"\n📦 Expected deliverable:")
        print(f"   {plan.expected_deliverable}")
        
        if plan.assumptions:
            print(f"\n⚠️  Assumptions:")
            for assumption in plan.assumptions:
                print(f"   - {assumption}")
        
        return plan
        
    except Exception as e:
        print(f"\n❌ Failed to create plan: {e}")
        import traceback
        traceback.print_exc()
        return None


def try_report_progress(plan):
    """Try reporting progress on a plan step."""
    if not plan:
        print("\n⏭️  Skipping progress test (no plan available)")
        return
    
    print("\n" + "=" * 80)
    print("EXPERIMENT 2: Reporting step progress")
    print("=" * 80)
    
    if not plan.steps:
        print("\n⏭️  No steps to report on")
        return
    
    first_step = plan.steps[0]
    print(f"\nReporting completion of step: {first_step.id}")
    
    try:
        message = report_step_progress(
            plan_id=plan.plan_id,
            step_id=first_step.id,
            status="completed",
            evidence={
                "urns": ["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)"],
                "selected_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)",
                "search_results": 3,
                "selection_reason": "Production environment, highest usage",
            },
            confidence=0.9,
        )
        
        print(f"\n✅ Progress reported!")
        print(f"   Response: {message}")
        
    except Exception as e:
        print(f"\n❌ Failed to report progress: {e}")
        import traceback
        traceback.print_exc()


def try_revise_plan(plan):
    """Try revising a plan when encountering issues."""
    if not plan:
        print("\n⏭️  Skipping revision test (no plan available)")
        return
    
    print("\n" + "=" * 80)
    print("EXPERIMENT 3: Revising a plan")
    print("=" * 80)
    
    if len(plan.steps) < 2:
        print("\n⏭️  Need at least 2 steps to test revision")
        return
    
    issue = "No Looker assets found in immediate downstream, only Snowflake tables"
    evidence = {
        "downstream_count": 8,
        "platforms": ["snowflake"],
        "max_hops": 2,
        "looker_found": 0,
    }
    
    print(f"\nIssue: {issue}")
    print(f"Completed steps: ['{plan.steps[0].id}']")
    print(f"Current step: {plan.steps[1].id}")
    
    try:
        revised = revise_plan(
            plan_id=plan.plan_id,
            completed_steps=[plan.steps[0].id],
            current_step=plan.steps[1].id,
            issue=issue,
            evidence=evidence,
        )
        
        print(f"\n✅ Plan revised successfully!")
        print(f"   Plan ID: {revised.plan_id} (same as original)")
        print(f"   Version: {revised.version} (was {plan.version})")
        print(f"   Number of steps: {len(revised.steps)}")
        
        print(f"\n📋 Revised steps:")
        for step in revised.steps:
            print(f"   {step.id}: {step.description}")
            if step.done_when:
                print(f"      Done when: {step.done_when}")
        
        if len(revised.assumptions) > len(plan.assumptions):
            print(f"\n📝 New assumptions added:")
            for assumption in revised.assumptions[len(plan.assumptions):]:
                print(f"   - {assumption}")
        
    except Exception as e:
        print(f"\n❌ Failed to revise plan: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run all planner experiments."""
    print("\n" + "🔧" * 40)
    print("DataHub Planning Tools - Experiment Script")
    print("🔧" * 40)
    
    # Experiment 1: Create plan
    plan = try_create_plan()
    
    # Experiment 2: Report progress
    try_report_progress(plan)
    
    # Experiment 3: Revise plan
    try_revise_plan(plan)
    
    print("\n" + "=" * 80)
    print("✅ All experiments completed!")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()

