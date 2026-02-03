# Snowflake Intelligence Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [← LangChain Integration](./langchain.md)

## What Problem Does This Solve?

Snowflake Intelligence provides powerful text-to-SQL capabilities, but it only sees raw table and column names. Without business context, your Snowflake Intelligence agents:
- ❌ Can't distinguish `customer_revenue` from `customer_revenue_archive`
- ❌ Don't understand business glossary terms like "churn" or "LTV"
- ❌ Can't discover "all tables about customers" across schemas
- ❌ Don't know which datasets are certified vs deprecated
- ❌ Have no context about data ownership or documentation

**DataHub's Snowflake Context Connector** solves this by providing Snowflake Intelligence with access to your DataHub metadata through User-Defined Functions (UDFs). This enables:

### What You Can Do
- ✅ **Semantic Search**: "Find all revenue tables owned by finance"
- ✅ **Business Context**: "What's the business definition of 'churn'?"
- ✅ **Quality Signals**: "Show me certified customer datasets"
- ✅ **Documentation Access**: Search across data docs and descriptions
- ✅ **Ownership Info**: Discover who owns and maintains datasets

### Example Queries DataHub Enables

```sql
-- Agent can now answer:
"Find all tables with customer data in the marketing domain"
"What datasets are tagged as PII?"
"Show me the business definition from the glossary for MRR"
"Which tables does the finance team own?"
```

## Overview

DataHub's Snowflake context connector allows creating [Snowflake Intelligence](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence) agents with UDFs that can access DataHub Context to power text-to-SQL generation. The connector creates an integration with DataHub that can search DataHub for documents and assets, as well as read Snowflake data tables and generate queries to help answer questions.

This guide and agent creation tool will create an end-to-end experience for setting up a Snowflake Intelligence agent that can be used in Snowflake Intelligence.

## Setup

### Install the add-on to acryl-datahub package

```bash
pip install datahub-agent-context[snowflake]
```

### Permissions

#### DataHub

In order to interact with DataHub, you will need the following from your DataHub account

- The URL of your DataHub Cloud instance e.g. `https://<tenant>.acryl.io`
- A [personal access token](../../authentication/personal-access-tokens.md)

#### Snowflake

In order to execute the SQL in Snowflake, you will need a user with the `SNOWFLAKE_ADMIN` role to configure the rules and UDFs. This is necessary to set up the secrets and networking options. Once the initial setup is completed, the `SNOWFLAKE_INTELLIGENCE_ADMIN` role is required to do edits and further configuration.

### Generate or Execute SQL for creating Snowflake Agent

In order to use DataHub tools in Snowflake intelligence, we'll create a new agent that has access to UDFs that can use the context inside of DataHub to answer using business questions. We can either run the SQL to register the agent and tools ourselves, or let the DataHub Agents CLI execute it for us.

#### Execute SQL for Snowflake directly

Add `--execute` to automatically execute the SQL as your Snowflake user.

Authentication:

- Use `--sf-password` to automatically run the generated scripts with your password or PAT.
- SSO support using an external browser with `--sf-authenticator externalBrowser`.

```bash
datahub agent create snowflake \
  --sf-account YOUR_ACCOUNT \
  --sf-user YOUR_USER \
  --sf-password YOUR_PASSWORD \
  --sf-role YOUR_ROLE \
  --sf-warehouse YOUR_WAREHOUSE \
  --sf-database YOUR_DATABASE \
  --sf-schema YOUR_SCHEMA \
  --datahub-url https://your-datahub.acryl.io \
  --datahub-token YOUR_TOKEN \
  --enable-mutations \
  --execute
```

This will automatically execute the commands in your Snowflake environment and output the results. This workflow is recommended for a hands off default experience.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-execute-generator.png"/>
</p>

#### Generate SQL for Snowflake

```bash
datahub agent create snowflake \
  --sf-account YOUR_ACCOUNT \
  --sf-user YOUR_USER \
  --sf-role YOUR_ROLE \
  --sf-warehouse YOUR_WAREHOUSE \
  --sf-database YOUR_DATABASE \
  --sf-schema YOUR_SCHEMA \
  --datahub-url https://your-datahub.acryl.io \
  --datahub-token YOUR_TOKEN \
  --enable-mutations
```

In this version you will need to execute 5 SQL files in your snowflake UI as a notebook. This is recommended for advanced workflows or if you want to review or make changes to the configuration before publishing. These configuration files can be pushed into version control if desired to maintain history.

```sql
-- 1. Set up configuration and secrets
@00_configuration.sql;

-- 2. Create network rules
@01_network_rules.sql;

-- 3. Create DataHub UDFs
@02_datahub_udfs.sql;

-- 4. Create stored procedure
@03_stored_procedure.sql;

-- 5. Create Cortex Agent
@04_cortex_agent.sql;

```

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-sql.png"/>
</p>

### Configure Snowflake agent in the UI

Once your agent is created, you can further customize it's prompt settings, models, tools, & more inside the Snowflake user interface. It is recommended to make sure your prompt and model is tweaked for your specific use case and requirements.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-cortex-agent.png"/>
</p>

### Using your DataHub Agent

Open [Snowflake Intelligence](https://ai.snowflake.com/) and select the DataHub Agent.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-intellgence.png"/>
</p>

## Updating UDFs

Periodically we expect to add new tools and update existing tools. To update the tools for your DataHub Agent, simply run the following SQL snippets to update the tool definitions and SDK:

```sql
-- 1. Create updated DataHub UDFs
@02_datahub_udfs.sql;

-- 2. Create updated Cortex Agent
@04_cortex_agent.sql;
```

After running these updates, refresh your Snowflake Intelligence UI to see the new tools.

## Agent Context

For more info on the tools exposed and DataHub Context, see the [DataHub Agent Context Documentation](./agent-context.md).

## Troubleshooting

### Setup Issues

#### Problem: Permission Denied Errors

**Symptoms**: `Insufficient privileges to operate on database/schema` errors during setup

**Solutions**:
- Verify you're using the `ACCOUNTADMIN` or `SECURITYADMIN` role for initial setup
- Check that your user has `CREATE DATABASE` and `CREATE INTEGRATION` privileges
- Ensure the role has `USAGE` on the warehouse
- Run: `SHOW GRANTS TO USER <your_user>;` to verify permissions

#### Problem: Network Rule Creation Fails

**Symptoms**: Cannot create network rules for DataHub connection

**Solutions**:
- Verify your Snowflake account allows external network access
- Check that the DataHub URL is accessible from your network
- Ensure you're using the correct DataHub URL format: `https://your-instance.acryl.io`
- Contact your Snowflake account admin if external network access is restricted

#### Problem: Secret Creation Fails

**Symptoms**: Cannot create secret for DataHub token

**Solutions**:
- Verify the DataHub token is valid and not expired
- Check that the token format is correct (should start with `eyJ`)
- Ensure the secret name doesn't conflict with existing secrets
- Run: `SHOW SECRETS IN SCHEMA <schema>;` to check existing secrets

### Agent Execution Issues

#### Problem: Agent Can't Find DataHub Tools

**Symptoms**: Agent doesn't use DataHub UDFs or says "function not found"

**Solutions**:
- Verify UDFs were created: `SHOW USER FUNCTIONS IN SCHEMA <schema>;`
- Check agent configuration includes DataHub tools
- Refresh the Snowflake Intelligence UI
- Recreate the agent using the latest `@04_cortex_agent.sql`

#### Problem: UDF Execution Errors

**Symptoms**: UDF calls fail with timeout or connection errors

**Solutions**:
- Verify network rules are configured correctly
- Test DataHub connectivity: Try calling a UDF directly in a SQL worksheet
- Check DataHub server is accessible and running
- Verify the DataHub token hasn't expired
- Review Snowflake query history for detailed error messages

#### Problem: Empty or No Results from DataHub

**Symptoms**: Agent says "no data found" even though data exists in DataHub

**Solutions**:
- Verify token has correct permissions in DataHub
- Check that entities exist and are searchable in DataHub UI
- Try the search directly in DataHub UI with the same query
- Verify the entity types exist (dataset, dashboard, etc.)
- Check DataHub search index is up to date

### Agent Quality Issues

#### Problem: Agent Doesn't Use DataHub Context

**Symptoms**: Agent generates SQL without consulting DataHub metadata

**Solutions**:
- Update the agent system prompt to explicitly mention using DataHub tools
- Configure agent to require tool usage before generating SQL
- Add examples of DataHub tool usage to the agent prompt
- Verify tools are properly registered in the agent configuration

#### Problem: Agent Hallucinates Table Names

**Symptoms**: Agent suggests tables that don't exist

**Solutions**:
- Ensure agent is configured to search DataHub before querying Snowflake
- Update prompt to require validation of table names via DataHub
- Limit agent to only use tables found in DataHub search results
- Add a validation step that checks table existence

### CLI Issues

#### Problem: `datahub agent create snowflake` Command Not Found

**Solutions**:
```bash
# Ensure package is installed with CLI
pip install --upgrade 'acryl-datahub[cli]'
pip install datahub-agent-context[snowflake]

# Verify installation
datahub version
datahub agent --help
```

#### Problem: Authentication Fails with `--execute`

**Solutions**:
- For password auth: Ensure password is properly quoted if it contains special characters
- For SSO: Use `--sf-authenticator externalbrowser` and ensure browser can open
- For key-pair auth: Use `--sf-private-key-path` with your key file
- Test connection manually: `snowsql -a <account> -u <user>`

### Performance Issues

#### Problem: Slow Agent Responses

**Solutions**:
- Ensure warehouse size is appropriate for workload (start with SMALL or MEDIUM)
- Check DataHub API response times in DataHub logs
- Optimize DataHub searches by adding specific entity type filters
- Consider caching frequently accessed metadata
- Review Snowflake query profile for bottlenecks

### Debugging Tips

#### Enable Verbose Logging

```bash
# When using --execute, add verbose flag
datahub agent create snowflake \
  --execute \
  --verbose \
  ...other flags...
```

#### Test UDF Directly

```sql
-- Test search UDF
SELECT datahub_search('customer', {'entity_type': ['dataset']}, 10);

-- Test document search
SELECT datahub_search_documents('data retention policy', 5);

-- Check UDF definitions
SHOW USER FUNCTIONS LIKE 'datahub%';
```

#### Check Agent Logs

In Snowflake Intelligence UI:
1. Open the agent conversation
2. Click "View Details" on a response
3. Check the "Tool Calls" section to see which UDFs were called
4. Review execution times and errors

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `Network rule violation` | DataHub URL not allowed | Update network rules to include DataHub domain |
| `Secret not found` | Secret name mismatch | Verify secret name matches in UDF and creation script |
| `Insufficient privileges` | Missing permissions | Grant required roles and privileges |
| `Invalid access token` | Token expired or invalid | Regenerate DataHub token and update secret |
| `Function does not exist` | UDF not created | Run `@02_datahub_udfs.sql` to create UDFs |

### Getting Help

- **DataHub Documentation**: [Agent Context Kit](./agent-context.md)
- **Snowflake Intelligence Docs**: [Getting Started Guide](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence)
- **Community Support**: [DataHub Slack](https://datahubspace.slack.com/)
- **GitHub Issues**: [Report bugs](https://github.com/datahub-project/datahub/issues)
- **DataHub Cloud Support**: Email support@acryl.io

### Verification Checklist

After setup, verify everything works:

- [ ] UDFs created successfully: `SHOW USER FUNCTIONS LIKE 'datahub%';`
- [ ] Network rules configured: `SHOW NETWORK RULES;`
- [ ] Secrets created: `SHOW SECRETS IN SCHEMA <schema>;`
- [ ] Agent registered: Check Snowflake Intelligence UI
- [ ] Test UDF directly: `SELECT datahub_search('test', {}, 5);`
- [ ] Test agent: Ask a simple question in Snowflake Intelligence
- [ ] Verify DataHub tools appear in agent configuration
