

# FAQ

> **Availability:** DataHub Cloud only

## What does DataHub do for data agents?

Two things. First, it activates and centralizes semantic context that's fragmented across your tools: semantic models, metric definitions, BI logic, documentation, and the patterns in your query history. It fills in what's missing and keeps it all current. Second, it makes that context easy to put to work: you can build agents with access to all of it, or only the part a team needs, to answer your business's most important questions.

## Is DataHub a semantic layer?

Not in the way that dbt semantic models, Snowflake semantic views, or Cube are. Those are explicit semantic layers: people define metrics and models by hand, and they remain the source of truth for your most important, best-governed metrics.

DataHub provides **inferred** semantic context. It extracts semantics from how your warehouse and data landscape are actually used, and turns them into a map that shows AI agents how similar questions have been answered before.

The two are complementary. DataHub brings your explicit semantic layer in, alongside everything else, and adds semantic context for the long tail of data domains that no one has modeled by hand. Your semantic layer stays the first stop for your core metrics; DataHub covers the rest, and keeps it reviewed and tested.

## We already have a semantic layer. Do we still need DataHub?

Only if your agent needs to go beyond the metrics your semantic layer defines, which most do. Bring your semantic layer into DataHub ([step 2](./ingest-context.md)), and DataHub builds on it.

## Why not build this ourselves?

Many teams start with a folder of prompts and Markdown files. That works for a single team, then breaks down: the context isn't tied to the tables it describes, it goes stale as your data changes, it lives in several places, and only engineers can maintain it.

DataHub ties every piece of context to the assets it describes, keeps it current from your existing tools and query history, verifies it with review and evals, and lets the people who know the data maintain it without writing code.

## Do we need a particular agent?

No. DataHub complements your agent stack rather than replacing it. Claude, ChatGPT, Databricks Genie, Snowflake Cortex and CoWork, LangChain, CrewAI, Google ADK, and custom agents can all use DataHub over MCP. [Step 4](./activate-context.md) covers three options: use Ask DataHub directly, connect your agent to a DataHub agent, or connect your agent to DataHub tools.

## We already have an evals platform. Why use DataHub evals?

DataHub evals answer a narrower question: can an agent find and use the right context in DataHub? That makes them the right tool for deciding whether a change to your context helps before it goes live. Keep your end-to-end agent evals in tools such as Langfuse or LangSmith as well. The two work well together, and you can report results from your own harness into DataHub.

## Can we use context we already have, such as documents in Git or a wiki?

Yes. Import it, or write it directly in DataHub. DataHub gives it one place to be reviewed, tested with evals, and published to your agents.

## Can agents search everything in natural language?

Context documents are searched by meaning, so an agent's question finds the right document even when the wording differs. Tables, metrics, and other assets are found through DataHub search, ranked by popularity, freshness, and the signals you set. Natural-language search for table and column descriptions is coming soon.

## Does DataHub read our data?

No. DataHub works from metadata: schemas, BI and semantic definitions, your documents, and the query logs from your warehouse. It never reads the data in your tables. When an agent runs SQL, the query runs in your warehouse, with the permissions you grant.

## Are query logs sent to a language model?

No. Query logs can contain literal values, such as a customer ID in a filter, so Context Generation doesn't send them to a language model. Instead, it groups queries deterministically by parsing their SQL, then generates documents that describe the general query pattern (the tables, joins, metrics, and filters involved) rather than any specific query.

## How is AI usage billed?

With AI Credits. AI features consume AI Credits as they run, including Context Generation, evals, Ask DataHub, and custom agents. Each eval run uses credits, because an agent answers the question and an AI judge grades it. Your DataHub Cloud subscription includes a bundle of AI Credits, which roughly corresponds to the underlying model usage. Contact your DataHub account team for details about your plan.

## Who needs which permissions?

| To...                                                             | You need                                           | Granted by default to |
| ----------------------------------------------------------------- | -------------------------------------------------- | --------------------- |
| Connect data sources and import documents                         | **Manage Metadata Ingestion**                      | Admins                |
| Create Context Generation jobs, MCP servers, and AI plugins       | **Manage Platform Settings**                       | Admins                |
| Create and run evals                                              | **Manage Evals**                                   | Admins and Editors    |
| Create custom agents, generate evals, and approve generated evals | **Manage Agents**                                  | Admins                |
| Review Context Feedback                                           | **Manage Context Feedback**                        | Admins and Editors    |
| Approve changes to a context document                             | **Manage Documents**, or ownership of the document | Admins and owners     |

## Can we use this with DataHub Core?

Partly. DataHub Core, the open-source edition, includes context documents, metrics and semantic models, and the [MCP server](../../features/feature-guides/mcp.md), so you can connect your own agent to DataHub tools, as in option c, without the SQL context tools that come with the add-on. Context Generation, evals, review workflows, custom agents, and scoped MCP servers require DataHub Cloud with the Context add-on.

## What's included in the Context add-on?

Context Generation, evals, review workflows for context changes, natural-language search for context documents, custom agents, and scoped MCP servers. Connecting your data, writing and importing documents, and the main [MCP server](../../features/feature-guides/mcp.md) are part of DataHub Cloud.
