

# Step 4: Activate Context

> **Availability:** DataHub Cloud only

:::info Context add-on
This step uses features from the DataHub Cloud **Context** add-on, currently in Public Beta.
:::

Your context is now centralized in DataHub, and your evals show it works. Now put it to work: give agents access to all of it, or only the part a team needs, so people can get answers to your business's most important questions. This step involves two decisions: how people reach DataHub, and what your agent can see.

## Choose how to activate

There are three ways to activate your context. They differ in who does the reasoning.

### a. [Use Ask DataHub directly](./use-ask-datahub.md)

People ask questions in DataHub, Slack, or Teams. DataHub finds the data, writes the SQL, and runs it.

- **Pros:** Works out of the box. Applies your DataHub agent instructions and domain scope. Nothing to build.
- **Cons:** People ask their questions in DataHub, Slack, or Teams, rather than in tools of their choosing.
- **Best for:** Getting started, and teams without an agent of their own.

### b. [Connect your agent to a DataHub agent](./connect-to-datahub-agent.md)

Your agent, such as Claude, ChatGPT, Copilot, or a LangChain app, gets a single tool for asking DataHub. It hands each data question to Ask DataHub or a custom DataHub agent, which does the data work and returns the answer.

- **Pros:** People stay in the agent they already use. Your agent stays simple. Your DataHub agent instructions and domain scope apply, so your data team improves answers in one place for everyone.
- **Cons:** Each question passes through the DataHub agent, which adds latency. Your agent has less say in how data is found. Each DataHub agent you expose needs its own MCP server.
- **Best for:** Organizations that already have an agent and want DataHub to own the data expertise.

### c. [Connect your agent to DataHub tools](./connect-to-datahub-tools.md)

Your agent gets DataHub's search and context tools, decides what to look up, and runs SQL through your warehouse's MCP server.

- **Pros:** Full control over how your agent reasons. No intermediate agent, so it can be faster and less costly per question.
- **Cons:** Doesn't apply your Ask DataHub or custom agent instructions, so your agent carries that knowledge itself, guided by DataHub's open-source [skills](https://github.com/datahub-project/datahub-skills). DataHub plugins for Claude and ChatGPT bundle the skills; other agents install them. More to set up: the skills, a warehouse MCP server, and evals reported from your own harness.
- **Best for:** Engineering teams that need to own the reasoning.

### At a glance

|                                        | **a. Ask DataHub directly** | **b. Your agent → DataHub agent** | **c. Your agent → DataHub tools** |
| -------------------------------------- | --------------------------- | --------------------------------- | --------------------------------- |
| **People ask in**                      | DataHub, Slack, Teams       | Your agent                        | Your agent                        |
| **Who does the reasoning**             | DataHub                     | DataHub                           | Your agent                        |
| **Applies DataHub agent instructions** | Yes                         | Yes                               | No                                |
| **Scope to a domain with**             | A custom agent              | A custom agent                    | A scoped MCP server and a View    |
| **SQL runs through**                   | DataHub AI plugins          | DataHub AI plugins                | Your warehouse's MCP server       |

**We recommend a or b.** If you don't have an agent of your own, start with **a**. If you do, choose **b**. Choose **c** only when you need full control over how your agent reasons. Many teams start with **a** and add **b** as people move into other tools.

## Choose what your agent can see

Each option works for both a domain agent and a global agent.

|                 | **Domain agent**                                       | **Global agent**                                            |
| --------------- | ------------------------------------------------------ | ----------------------------------------------------------- |
| **Sees**        | One domain's tables, metrics, and context documents    | Everything the person asking can see                        |
| **Best for**    | A business team that needs the right number every time | Analysts and engineers exploring the long tail of your data |
| **Scoped with** | A domain, and optionally a View                        | Nothing; this is the default                                |

### What to set up

Find your activation option and the kind of agent you want:

|                                                  | **a. Ask DataHub directly**              | **b. Your agent → DataHub agent**                         | **c. Your agent → DataHub tools**                    |
| ------------------------------------------------ | ---------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------- |
| **Global agent** (all data and context)          | Use Ask DataHub as-is                    | An MCP server that exposes **Ask DataHub**                | The main MCP server, `https://<tenant>.acryl.io/mcp` |
| **Domain agent** (one domain's data and context) | A custom agent with **Scope: By Domain** | An MCP server that exposes your domain's **custom agent** | A scoped MCP server with a View for the domain       |

With any option, you can also guide an agent toward the right domain with instructions, such as which domain to search for which kinds of questions.

### Package a domain for a domain agent

1. **Create the domain.** Under **Domains**, create it, such as `Finance`, and add its owners.
2. **Add its assets.** From the domain's page, add the tables, metrics, semantic models, and dashboards that belong to it. See [Domains](../../domains.md).
3. **Add its documents.** Set the domain on documents you write or import. For generated documents, use **Assign generated docs to** on your [Context Generation](./generate-context.md#1-create-a-job) job.
4. **Assign its evals.** Set each eval's **Domain**, so you can run and track the domain's evals on their own.
5. **Expose only that domain.** For options **a** and **b**, create a custom agent and set **Scope** to **By Domain**. For option **c**, create a scoped MCP server and, under **Scope to View**, select a [View](../../features/feature-guides/views/overview.md) that filters to the domain and includes documents. A View can also filter by database or schema, if that's how your data is organized. Alternatively, for any option, add instructions that tell the agent which domain to search for which kinds of questions.

:::tip
Anything not assigned to the domain is invisible to a domain agent. If the agent can't find something it should, check the asset's domain first.
:::

### Expose all data and context to a global agent

1. **Connect broadly.** Ingest all of your warehouses with query history, along with your BI tools, semantic layer, and documentation.
2. **Generate context across domains.** Create a Context Generation job for each domain or major schema.
3. **Expose everything.** Leave the scope unset. For options **a** and **b**, use Ask DataHub itself. For option **c**, use the main MCP server at `https://<tenant>.acryl.io/mcp`.

A global agent sees whatever the person asking can see, so access remains governed by your [policies](../../authorization/policies.md). If your organization has a default View, it narrows what Ask DataHub sees, and it always applies in Slack and Teams.

## Shared setup

### Connect your agent

Options **b** and **c** connect your agent to DataHub over [MCP](../../features/feature-guides/mcp.md). Use the MCP server URL from your option's page, and follow the guide for your agent. For option **c**, the DataHub plugins for [Claude](https://claude.com/marketplace/plugins) and [ChatGPT](https://chatgpt.com/plugins) also bundle DataHub's [skills](https://github.com/datahub-project/datahub-skills).

| Agent                                                                              | How to connect                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Claude** (web, desktop, Team, Enterprise)                                        | Add DataHub as a custom connector. On Team and Enterprise plans, an owner can add it once for the whole organization. [Setup](../../features/feature-guides/mcp.md#configure-your-client)                                                                                                                                                                                   |
| **ChatGPT**                                                                        | Add DataHub as a connector. [Setup](../../features/feature-guides/mcp.md#configure-your-client)                                                                                                                                                                                                                                                                             |
| **Claude Code, Cursor**                                                            | Add the MCP server URL. [Claude Code](../../dev-guides/agent-context/claude.md), [Cursor](../../dev-guides/agent-context/cursor.md)                                                                                                                                                                                                                                         |
| **Snowflake Cortex and CoWork, Databricks Genie and Agent Bricks, Copilot Studio** | [Snowflake Cortex](../../dev-guides/agent-context/snowflake.md), [Snowflake CoWork](../../features/feature-guides/mcp.md#configure-your-client), [Databricks Genie](../../dev-guides/agent-context/databricks-genie-code.md), [Agent Bricks](../../dev-guides/agent-context/databricks-agent-bricks.md), [Copilot Studio](../../dev-guides/agent-context/copilot-studio.md) |
| **LangChain, CrewAI, Google ADK, or custom code**                                  | Connect over MCP, or use the `datahub-agent-context` Python package. [LangChain](../../dev-guides/agent-context/langchain.md), [Google ADK](../../dev-guides/agent-context/google-adk.md)                                                                                                                                                                                   |

Guides for more platforms are in the [Agent Context Kit](../../dev-guides/agent-context/agent-context.md).

### Decide how people sign in

- **People chatting with an agent** sign in with their own DataHub account, using OAuth and your SSO provider. The agent sees only what that person can see.
- **Agents that run unattended,** such as scheduled jobs, bots, and backend services, use a [service account](../../features/feature-guides/service-accounts.md) token with only the access they need.

### Keep SQL read-only

Give your agent a read-only warehouse role. It never needs to write data.

### Understand what scoping does

Scoping an agent to a domain changes what it looks at by default. It doesn't prevent anyone from seeing data they can already access in other ways. To restrict access, use [policies](../../authorization/policies.md).
