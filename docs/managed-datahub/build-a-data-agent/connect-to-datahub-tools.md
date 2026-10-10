

# c. Connect Your Agent to DataHub Tools

> **Availability:** DataHub Cloud only

:::info Context add-on
Scoped MCP servers and the SQL context tools are part of the DataHub Cloud **Context** add-on, currently in Public Beta.
:::

Your agent, such as Claude, Cursor, a LangChain or CrewAI app, or a custom agent, gets DataHub's search and context tools. It decides what to look up in DataHub (tables, metrics, definitions, and past queries), then runs SQL through your warehouse's own MCP server.

**Choose this option if** you need full control over how your agent reasons. Because there's no intermediate agent, it can also be faster and less costly per question.

**Keep in mind:** this option doesn't apply the instructions you've given Ask DataHub or your custom agents. Your agent carries that knowledge itself. For most teams with their own agent, we recommend [option b](./connect-to-datahub-agent.md), which keeps data expertise in DataHub, where your data team maintains it.

**What you'll set up:** a DataHub MCP connection, a skill that teaches your agent how to use it, and your warehouse's MCP server.

## 1. Connect to DataHub

- **For a domain agent,** create a scoped MCP server under **Settings > AI > MCP Servers**. Under **Scope to View**, select a View that covers your domain, and add instructions for it. Make sure your documents are assigned to the domain, or the server won't see them. See [Scoped MCP Servers](../../features/feature-guides/scoped-mcp-servers.md).
- **For a global agent,** use the main MCP server at `https://<tenant>.acryl.io/mcp`. Your agent can search everything the signed-in person can see.

See [Connect your agent](./activate-context.md#connect-your-agent) for setup on your platform.

## 2. Teach your agent to use DataHub

Tools alone aren't enough. Without guidance, an agent tends to search repeatedly and then fill the gaps with guesses. DataHub's open-source skills, in the [datahub-skills](https://github.com/datahub-project/datahub-skills) repository, teach your agent how to use DataHub well. For analytics agents, the key one is [`datahub-sql-workflow`](https://github.com/datahub-project/datahub-skills/tree/main/skills/datahub-sql-workflow), which teaches a reliable sequence:

1. Look for how analysts have answered similar questions before.
2. Read your team's documents for definitions and known issues.
3. Confirm the candidate tables using trust signals: owners, lineage, usage, and past queries.
4. Check columns, join keys, and grain before writing SQL.
5. Run one read-only query, cite its sources, and [report any missing context](./improve-with-feedback.md).

Choose how to install the skills:

| Your agent                                              | How to get the skills                                                                                                                                               |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Claude**                                              | Install the DataHub plugin from the [Claude plugin marketplace](https://claude.com/marketplace/plugins). It bundles the DataHub connector and skills.               |
| **ChatGPT**                                             | Install the DataHub plugin from [ChatGPT plugins](https://chatgpt.com/plugins). It bundles the DataHub connector and skills.                                        |
| **Claude Code**                                         | Run `claude plugin install datahub-skills`.                                                                                                                         |
| **Cursor, Codex, GitHub Copilot, Gemini CLI, Windsurf** | Run `npx skills add datahub-project/datahub-skills -a <agent>`, for example `-a cursor`.                                                                            |
| **LangChain, CrewAI, or a custom agent**                | Add the contents of the skill files from the [repository](https://github.com/datahub-project/datahub-skills) to your agent's system prompt. They're plain Markdown. |

:::note
If you use a scoped MCP server, leave its **Tools** empty to include everything, or make sure the SQL context tools are selected. The skill depends on them.
:::

## 3. Let your agent run SQL

Connect your agent to your warehouse's MCP server alongside DataHub's, using a read-only role:

- [Snowflake](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp)
- [BigQuery](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp)
- [Databricks](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp)
- [Redshift](https://github.com/awslabs/mcp)

Your agent now has two connections: DataHub to decide what to query, and your warehouse to run it. The skill always consults DataHub first.

## 4. Test with evals

Because your agent runs outside DataHub, it answers your evals and reports the results back:

- **In DataHub,** set each eval's **Eval Runner** to **External**. Your test harness asks your agent each question and sends the answer to DataHub for grading.
- **In code,** keep evals in YAML and run them in CI with the [evals CLI](../../cli-commands/evals.md).

## Check your work

- Your evals show a pass rate you're comfortable sharing.
- A question from outside the domain gets a clear "I can't answer that," not a guess.
- A small group of users has tried it for a week, including with difficult questions.

**Next:** [Step 5: Context feedback & improvement](./improve-with-feedback.md)
