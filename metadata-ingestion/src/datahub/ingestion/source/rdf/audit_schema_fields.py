#!/usr/bin/env python3
"""
Audit script to check schema field declarations across all BCBS239 domain files.
"""

import subprocess
from pathlib import Path


def run_dry_run(file_path):
    """Run dry-run on a single file and extract schema field information."""
    try:
        result = subprocess.run(
            ["python", "-m", "rdf", "--source", file_path, "--dry-run"],
            capture_output=True,
            text=True,
            cwd="/Users/stephengoldbaum/Code/rdf",
        )

        if result.returncode != 0:
            return None, f"Error running {file_path}: {result.stderr}"

        output = result.stdout
        datasets = []

        # Parse the output to extract dataset information
        lines = output.split("\n")
        current_dataset = None

        for line in lines:
            line = line.strip()

            # Start of a new dataset
            if "Dataset:" in line and line.strip().startswith(
                ("1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9.")
            ):
                if current_dataset:
                    datasets.append(current_dataset)
                # Extract dataset name after "Dataset:"
                dataset_name = line.split("Dataset:")[1].strip()
                current_dataset = {"name": dataset_name, "fields": [], "field_count": 0}

            # Schema fields count
            elif line.startswith("Schema Fields:") and current_dataset:
                field_count_str = line.split(":")[1].strip().split()[0]
                try:
                    current_dataset["field_count"] = int(field_count_str)
                except ValueError:
                    current_dataset["field_count"] = 0

            # Individual field
            elif line.startswith("- ") and current_dataset:
                field_name = line.replace("- ", "").split(":")[0].strip()
                current_dataset["fields"].append(field_name)

        # Add the last dataset
        if current_dataset:
            datasets.append(current_dataset)

        return datasets, None

    except Exception as e:
        return None, f"Exception running {file_path}: {str(e)}"


def main():
    """Main audit function."""
    bcbs239_dir = Path("/Users/stephengoldbaum/Code/rdf/examples/bcbs239")

    # Files that define datasets
    dataset_files = [
        "accounts.ttl",
        "commercial_lending.ttl",
        "consumer_lending.ttl",
        "counterparty_master.ttl",
        "derivatives_trading.ttl",
        "equity_trading.ttl",
        "finance.ttl",
        "fixed_income_trading.ttl",
        "loan_hub.ttl",
        "market_data.ttl",
        "regulatory.ttl",
        "risk.ttl",
        "security_master.ttl",
    ]

    print("=" * 80)
    print("BCBS239 SCHEMA FIELD AUDIT")
    print("=" * 80)

    total_datasets = 0
    total_fields = 0
    issues = []

    for file_name in dataset_files:
        file_path = bcbs239_dir / file_name

        if not file_path.exists():
            print(f"❌ File not found: {file_name}")
            continue

        print(f"\n📁 {file_name}")
        print("-" * 50)

        datasets, error = run_dry_run(str(file_path))

        if error:
            print(f"❌ Error: {error}")
            issues.append(f"{file_name}: {error}")
            continue

        if not datasets:
            print("⚠️  No datasets found")
            continue

        for dataset in datasets:
            total_datasets += 1
            total_fields += dataset["field_count"]

            status = "✅" if dataset["field_count"] > 0 else "❌"
            print(f"{status} {dataset['name']}: {dataset['field_count']} fields")

            if dataset["field_count"] == 0:
                issues.append(f"{file_name} - {dataset['name']}: No schema fields")
            elif dataset["field_count"] < 5:
                issues.append(
                    f"{file_name} - {dataset['name']}: Only {dataset['field_count']} fields (suspiciously low)"
                )

            # Show first few fields
            if dataset["fields"]:
                for field in dataset["fields"][:5]:
                    print(f"   - {field}")
                if len(dataset["fields"]) > 5:
                    print(f"   ... and {len(dataset['fields']) - 5} more")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total datasets: {total_datasets}")
    print(f"Total schema fields: {total_fields}")
    print(f"Issues found: {len(issues)}")

    if issues:
        print("\n🚨 ISSUES:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("\n✅ No issues found!")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
