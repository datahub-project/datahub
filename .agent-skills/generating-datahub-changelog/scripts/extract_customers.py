#!/usr/bin/env python3
"""
Extract customer information from Linear ticket JSON data.

Usage:
    # Pipe Linear API JSON output:
    echo '[{"identifier": "CUS-123", "labels": ["Company: Acme"], ...}]' | python extract_customers.py

    # Or with a file:
    python extract_customers.py < tickets.json

Output (JSON):
    {
      "ING-1282": {"customer": "Acme", "source": "label:Company:"},
      "CUS-6615": {"customer": "Globex", "source": "email:@globex.example"},
      "ING-1304": {"customer": null, "source": null}
    }

Output (text, with --text flag):
    ING-1282: Acme (label:Company:)
    CUS-6615: Globex (email:@globex.example)
    ING-1304: (no customer)
"""

import json
import re
import sys

# Generic email domains to skip
GENERIC_DOMAINS = {
    'gmail.com', 'outlook.com', 'hotmail.com', 'yahoo.com',
    'icloud.com', 'protonmail.com', 'live.com', 'msn.com',
    'datahub.com', 'acryl.io', 'acryldata.io'  # Internal domains
}

# Email pattern
EMAIL_PATTERN = re.compile(r'[\w.+-]+@([\w-]+\.[\w.-]+)')


def extract_customer_from_labels(labels: list) -> tuple[str, str] | tuple[None, None]:
    """Extract customer from labels array."""
    if not labels:
        return None, None

    for label in labels:
        if not isinstance(label, str):
            # Handle label objects with 'name' field
            if isinstance(label, dict):
                label = label.get('name', '')
            else:
                continue

        # Check for "Company: X" format
        if label.startswith('Company:'):
            customer = label.split(':', 1)[1].strip()
            return customer, 'label:Company:'

        # Check for "acryl-<customer>" format
        if label.startswith('acryl-') and label != 'acryl-data':
            customer = label.replace('acryl-', '').replace('-', ' ').title()
            return customer, f'label:{label}'

    return None, None


def extract_customer_from_email(text: str) -> tuple[str, str] | tuple[None, None]:
    """Extract customer from email domains in text."""
    if not text:
        return None, None

    matches = EMAIL_PATTERN.findall(text)
    for domain in matches:
        domain_lower = domain.lower()
        if domain_lower not in GENERIC_DOMAINS:
            # Extract company name from domain
            company = domain.split('.')[0].title()
            return company, f'email:@{domain_lower}'

    return None, None


def extract_customer_from_zendesk(attachments: list) -> tuple[str, str] | tuple[None, None]:
    """Check if ticket has Zendesk attachments (indicates customer issue)."""
    if not attachments:
        return None, None

    for att in attachments:
        url = att.get('url', '') if isinstance(att, dict) else ''
        if 'zendesk.com' in url:
            return '[Zendesk]', 'zendesk'

    return None, None


def extract_customer_from_project(project: str) -> tuple[str, str] | tuple[None, None]:
    """Check if ticket is in a customer project."""
    if not project:
        return None, None

    customer_projects = {'Customer Issues', 'Customer Feature Requests', 'Customer Success'}
    if project in customer_projects:
        return '[Customer Project]', f'project:{project}'

    return None, None


def extract_customer(ticket: dict) -> dict:
    """Extract customer info from a Linear ticket using priority order."""
    identifier = ticket.get('identifier', 'unknown')

    # Priority 1: CUS- prefix (automatic customer)
    if identifier.startswith('CUS-'):
        # Still try to get actual customer name from other sources
        customer, source = extract_customer_from_labels(ticket.get('labels', []))
        if customer:
            return {'customer': customer, 'source': source, 'auto': 'CUS-prefix'}

        customer, source = extract_customer_from_email(ticket.get('description', ''))
        if customer:
            return {'customer': customer, 'source': source, 'auto': 'CUS-prefix'}

        # CUS- but no name found - still mark as customer
        return {'customer': '[Customer Issue]', 'source': 'CUS-prefix', 'auto': 'CUS-prefix'}

    # Priority 2: Labels with Company: or acryl-
    customer, source = extract_customer_from_labels(ticket.get('labels', []))
    if customer:
        return {'customer': customer, 'source': source, 'auto': None}

    # Priority 3: Email domains in description
    customer, source = extract_customer_from_email(ticket.get('description', ''))
    if customer:
        return {'customer': customer, 'source': source, 'auto': None}

    # Priority 4: Zendesk attachments
    customer, source = extract_customer_from_zendesk(ticket.get('attachments', []))
    if customer:
        return {'customer': customer, 'source': source, 'auto': None}

    # Priority 5: Customer project
    customer, source = extract_customer_from_project(ticket.get('project', ''))
    if customer:
        return {'customer': customer, 'source': source, 'auto': None}

    return {'customer': None, 'source': None, 'auto': None}


def main():
    text_mode = '--text' in sys.argv

    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON input - {e}", file=sys.stderr)
        sys.exit(1)

    # Handle single ticket or array
    if isinstance(data, dict):
        data = [data]

    if not isinstance(data, list):
        print("Error: Expected JSON object or array of tickets", file=sys.stderr)
        sys.exit(1)

    result = {}
    for ticket in data:
        identifier = ticket.get('identifier', 'unknown')
        info = extract_customer(ticket)
        result[identifier] = info

    if text_mode:
        for identifier, info in sorted(result.items()):
            customer = info['customer']
            source = info['source']
            auto = info.get('auto')

            if customer:
                auto_note = f" [AUTO:{auto}]" if auto else ""
                print(f"{identifier}: {customer} ({source}){auto_note}")
            else:
                print(f"{identifier}: (no customer)")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
