---
name: DataHub Token Generator
description: Generate authentication tokens for DataHub instances (Xero, Miro, etc.) with configurable validity periods
---

# DataHub Token Generator

This skill helps generate authentication tokens for DataHub SaaS instances in the datahub-apps repository.

## When to Use This Skill

- User asks to generate a token for a DataHub instance
- User mentions generating tokens for customers like Xero, Miro, Notion, etc.
- User needs authentication credentials for DataHub instances

## Prerequisites

1. Must be in the datahub-apps repository directory: `/Users/alex/work/datahub-apps`
2. AWS SSO authentication must be configured
3. Virtual environment must be set up with dependencies

## Steps to Generate a Token

### 1. Ensure Dependencies are Installed

First, check if the virtual environment exists and has all dependencies:

```bash
cd /Users/alex/work/datahub-apps
# If venv needs setup or refresh
UV_VENV_CLEAR=1 bash .github/scripts/install_deps.sh
```

### 2. Authenticate with AWS SSO

The token generation requires AWS credentials:

```bash
export AWS_PROFILE=acryl-read-write
aws sso login
```

This will open a browser window for authentication. Wait for successful login confirmation.

### 3. Generate the Token

Use the CLI command with the appropriate parameters:

```bash
cd /Users/alex/work/datahub-apps
source venv/bin/activate
export AWS_PROFILE=acryl-read-write
python helpers/cli.py customer get-dh-token --file-filter <INSTANCE> --validity <VALIDITY>
```

**Parameters:**

- `--file-filter`: The instance identifier (e.g., "xero", "miro", "748f3fc9fe-xero")
  - If multiple instances match (e.g., "xero" matches both "xero-dev" and "xero"), the command will list options
  - Use the full namespace ID to be specific (e.g., "748f3fc9fe-xero" for production)
- `--validity`: Token expiration period, one of:
  - `ONE_HOUR`
  - `ONE_DAY`
  - `ONE_MONTH`
  - `THREE_MONTHS`

### 4. Token Location

The generated token is automatically saved to `~/.datahubenv` and configured for the specified instance URL.

## Examples

### Example 1: Generate 3-month token for Xero production

```bash
cd /Users/alex/work/datahub-apps
source venv/bin/activate
export AWS_PROFILE=acryl-read-write
python helpers/cli.py customer get-dh-token --file-filter 748f3fc9fe-xero --validity THREE_MONTHS
```

### Example 2: Generate 1-day token for development instance

```bash
cd /Users/alex/work/datahub-apps
source venv/bin/activate
export AWS_PROFILE=acryl-read-write
python helpers/cli.py customer get-dh-token --file-filter xero-dev --validity ONE_DAY
```

## Common Issues and Solutions

### Issue: "ModuleNotFoundError: No module named 'croniter'"

**Solution:** Run the dependency installation script:

```bash
cd /Users/alex/work/datahub-apps
UV_VENV_CLEAR=1 bash .github/scripts/install_deps.sh
```

### Issue: "NoCredentialsError: Unable to locate credentials"

**Solution:** Authenticate with AWS SSO:

```bash
export AWS_PROFILE=acryl-read-write
aws sso login
```

### Issue: "Found multiple cases, please be more specific"

**Solution:** Use the full namespace ID instead of just the customer name. The error message will show all matching options.

## Important Notes

- Always activate the virtual environment before running commands
- Always set AWS_PROFILE before generating tokens
- Tokens are stored in `~/.datahubenv` and will overwrite any existing token configuration
- The token validity period starts from generation time
- For production instances, prefer longer validity periods (THREE_MONTHS) to reduce regeneration frequency
