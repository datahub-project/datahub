// note: to handle errors where you don't want a markdown file in the sidebar, add it as a comment.
// this will fix errors like `Error: File not accounted for in sidebar: ...`
// smoke-test/tests/library_examples/README.md

const fs = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// Ingestion source grouping
//
// The connector pages under docs/generated/ingestion/sources/ are produced by
// docgen. Rather than hand-listing ~110 of them here (which goes stale every
// time a connector is added), the groups below are derived at build time from
// the `platform_type` field in the connector catalog -- the same field that
// drives the filters on the /integrations page, so the two stay consistent.
//
// Files are never moved: grouping lives only in the sidebar, so a connector can
// be recategorised without changing its URL.
// ---------------------------------------------------------------------------

const SOURCES_ID_PREFIX = "docs/generated/ingestion/sources";
const SOURCES_DIR = path.join(__dirname, "..", SOURCES_ID_PREFIX);
const CONNECTOR_CATALOG = require("../metadata-ingestion/docs/sources/integrations_catalog.json");

// Render order. "Other" catches anything without a usable platform_type so a
// new connector is always reachable, never silently dropped from the nav.
const SOURCE_GROUP_ORDER = [
  "Databases & Warehouses",
  "Data Lakes & Query Engines",
  "BI & Visualization",
  "ETL, Transformation & Orchestration",
  "Streaming & Messaging",
  "AI & ML",
  "Files, Schemas & Formats",
  "Catalogs, Governance & Quality",
  "Identity & Access",
  "Business & Collaboration Apps",
  "DataHub Utilities",
  "Other",
];

const PLATFORM_TYPE_TO_GROUP = {
  Database: "Databases & Warehouses",
  "Data Lake": "Data Lakes & Query Engines",
  "BI Tool": "BI & Visualization",
  "ETL/ELT": "ETL, Transformation & Orchestration",
  Orchestrator: "ETL, Transformation & Orchestration",
  Messaging: "Streaming & Messaging",
  "AI+ML": "AI & ML",
  "Metadata Systems": "Files, Schemas & Formats",
  "Data Catalog": "Catalogs, Governance & Quality",
  "Data Quality": "Catalogs, Governance & Quality",
  "Identity Provider": "Identity & Access",
  Collaboration: "Business & Collaboration Apps",
  CRM: "Business & Collaboration Apps",
  Internal: "DataHub Utilities",
};

// platform_type is a single free-text field and is too coarse in two places:
// object stores and query engines are all tagged "Database", and a few
// governance tools are tagged "Metadata Systems". Redraw those here.
const SOURCE_GROUP_OVERRIDES = {
  abs: "Data Lakes & Query Engines",
  athena: "Data Lakes & Query Engines",
  "delta-lake": "Data Lakes & Query Engines",
  dremio: "Data Lakes & Query Engines",
  "fabric-onelake": "Data Lakes & Query Engines",
  gcs: "Data Lakes & Query Engines",
  hive: "Data Lakes & Query Engines",
  "hive-metastore": "Data Lakes & Query Engines",
  iceberg: "Data Lakes & Query Engines",
  presto: "Data Lakes & Query Engines",
  s3: "Data Lakes & Query Engines",
  trino: "Data Lakes & Query Engines",
  bigid: "Catalogs, Governance & Quality",
  dataplex: "Catalogs, Governance & Quality",
  glue: "Catalogs, Governance & Quality",
  odcs: "Catalogs, Governance & Quality",
};

// Integrations documented outside the generated tree (push-based connectors and
// schema tooling). Listed explicitly because docgen does not produce them.
const EXTRA_SOURCE_ENTRIES = {
  "ETL, Transformation & Orchestration": [
    { type: "doc", id: "docs/lineage/airflow", label: "Airflow" },
    { type: "doc", id: "docs/lineage/dagster", label: "Dagster" },
    { type: "doc", id: "docs/lineage/prefect", label: "Prefect" },
    {
      type: "doc",
      id: "metadata-integration/java/acryl-spark-lineage/README",
      label: "Spark",
    },
  ],
  "Streaming & Messaging": [
    // Discoverability alias: "Amazon Data Firehose" is ingested by the same
    // `kinesis` connector, but users searching for "Firehose" expect their own
    // entry. `ref` lets docusaurus point at the same doc twice.
    {
      type: "ref",
      id: "docs/generated/ingestion/sources/kinesis",
      label: "Amazon Data Firehose",
    },
  ],
  "Files, Schemas & Formats": [
    {
      type: "doc",
      id: "metadata-integration/java/datahub-protobuf/README",
      label: "Protobuf Schemas",
    },
  ],
  "Catalogs, Governance & Quality": [
    {
      type: "doc",
      id: "metadata-ingestion/integration_docs/great-expectations",
      label: "Great Expectations",
    },
  ],
};

function groupForConnector(platformId) {
  if (SOURCE_GROUP_OVERRIDES[platformId]) {
    return SOURCE_GROUP_OVERRIDES[platformId];
  }
  const entry = CONNECTOR_CATALOG[platformId];
  let rawType = entry && entry.platform_type;
  if (Array.isArray(rawType)) {
    rawType = rawType[0];
  }
  return PLATFORM_TYPE_TO_GROUP[rawType] || "Other";
}

function buildSourceGroups() {
  // If docgen has not run yet there is nothing to group; fall back to letting
  // docusaurus enumerate the directory so the build still works.
  if (!fs.existsSync(SOURCES_DIR)) {
    return [{ type: "autogenerated", dirName: SOURCES_ID_PREFIX }];
  }

  const grouped = {};
  for (const file of fs.readdirSync(SOURCES_DIR)) {
    if (!file.endsWith(".md")) continue;
    const platformId = file.slice(0, -3);
    const group = groupForConnector(platformId);
    (grouped[group] = grouped[group] || []).push(
      `${SOURCES_ID_PREFIX}/${platformId}`
    );
  }
  for (const [group, entries] of Object.entries(EXTRA_SOURCE_ENTRIES)) {
    (grouped[group] = grouped[group] || []).push(...entries);
  }

  const known = new Set(SOURCE_GROUP_ORDER);
  const order = [
    ...SOURCE_GROUP_ORDER,
    ...Object.keys(grouped).filter((g) => !known.has(g)),
  ];

  return order
    .filter((group) => grouped[group] && grouped[group].length)
    .map((group) => ({
      type: "category",
      label: group,
      collapsed: true,
      items: grouped[group].sort((a, b) => {
        const key = (x) => (typeof x === "string" ? x : x.label || x.id);
        return key(a).localeCompare(key(b));
      }),
    }));
}

module.exports = {
  overviewSidebar: [
    // Getting Started.
    {
      type: "html",
      value: "<div>Getting Started</div>",
      defaultStyle: true,
    },
    {
      type: "doc",
      label: "Quickstart",
      id: "docs/quickstart",
    },
    {
      label: "What Is DataHub?",
      type: "category",
      collapsed: true,
      link: { type: "doc", id: "docs/features" },
      items: [
        // By the end of this section, readers should understand the core use cases that DataHub addresses,
        // target end-users, high-level architecture, & hosting options
        {
          type: "doc",
          label: "Core Concepts",
          id: "docs/what-is-datahub/datahub-concepts",
        },
        {
          type: "link",
          label: "Demo",
          href: "https://demo.datahub.com/",
        },
        {
          type: "link",
          label: "Customer Stories",
          href: "https://datahub.com/resources/?2004611554=dh-stories",
        },
      ],
    },
    {
      type: "doc",
      label: "Use Docs with AI Tools",
      id: "docs/use-docs-with-ai",
    },
    // Capabilities.
    {
      type: "html",
      value: "<div>Capabilities</div>",
      defaultStyle: true,
    },
    {
      label: "Discovery & Search",
      type: "category",
      collapsed: true,
      link: {
        type: "generated-index",
        title: "Discovery & Search",
        description:
          "Find the data you need and understand where it came from — search, browse, lineage, and usage history.",
      },
      items: [
        {
          label: "Search",
          type: "doc",
          id: "docs/how/search",
        },
        {
          label: "Search Access Controls",
          type: "doc",
          id: "docs/features/feature-guides/search-access-controls",
          className: "saasOnly",
        },
        {
          label: "Views",
          type: "doc",
          id: "docs/features/feature-guides/views/overview",
        },
        {
          label: "Lineage",
          type: "category",
          link: {
            type: "doc",
            id: "docs/features/feature-guides/lineage",
          },
          items: [
            {
              label: "Automatic Lineage Extraction",
              type: "doc",
              id: "docs/generated/lineage/automatic-lineage-extraction",
            },
            {
              label: "Managing Lineage via UI",
              type: "doc",
              id: "docs/features/feature-guides/ui-lineage",
            },
            {
              label: "Lineage Impact Analysis",
              type: "doc",
              id: "docs/act-on-metadata/impact-analysis",
            },
            {
              type: "doc",
              id: "docs/lineage/openlineage",
              label: "OpenLineage",
            },
          ],
        },
        {
          label: "Dataset Usage & Query History",
          type: "doc",
          id: "docs/features/dataset-usage-and-query-history",
        },
        {
          label: "Schema History",
          type: "doc",
          id: "docs/schema-history",
        },
        {
          label: "Sync Status",
          type: "doc",
          id: "docs/sync-status",
        },
      ],
    },
    {
      label: "Governance & Organization",
      type: "category",
      collapsed: true,
      link: {
        type: "generated-index",
        title: "Governance & Organization",
        description:
          "Structure your metadata and control who can do what — domains, glossary, ownership, policies, and compliance.",
      },
      items: [
        {
          label: "Domains",
          type: "doc",
          id: "docs/domains",
        },
        {
          label: "Data Products",
          type: "doc",
          id: "docs/dataproducts",
        },
        {
          label: "Applications",
          type: "doc",
          id: "docs/features/feature-guides/applications",
        },
        {
          label: "Business Glossary",
          type: "doc",
          id: "docs/glossary/business-glossary",
        },
        {
          label: "Business Attributes",
          type: "doc",
          id: "docs/businessattributes",
        },
        {
          label: "Tags",
          type: "doc",
          id: "docs/tags",
        },
        {
          label: "Properties",
          type: "category",
          collapsed: true,
          items: [
            {
              label: "Overview",
              type: "doc",
              id: "docs/features/feature-guides/properties/overview",
            },
            {
              type: "doc",
              id: "docs/features/feature-guides/properties/create-a-property",
            },
          ],
        },
        {
          label: "Ownership",
          type: "doc",
          id: "docs/ownership/ownership-types",
        },
        {
          label: "Policies",
          type: "doc",
          id: "docs/authorization/access-policies-guide",
        },
        {
          label: "Data Access Roles",
          type: "doc",
          id: "docs/features/feature-guides/access-roles",
        },
        {
          label: "Service Accounts",
          type: "doc",
          id: "docs/features/feature-guides/service-accounts",
        },
        {
          label: "Compliance Forms",
          type: "category",
          collapsed: true,
          items: [
            {
              label: "Overview",
              type: "doc",
              id: "docs/features/feature-guides/compliance-forms/overview",
            },
            {
              type: "doc",
              id: "docs/features/feature-guides/compliance-forms/create-a-form",
            },
            {
              type: "doc",
              id: "docs/features/feature-guides/compliance-forms/complete-a-form",
            },
            {
              type: "doc",
              id: "docs/features/feature-guides/compliance-forms/analytics",
              className: "saasOnly",
            },
          ],
        },
        {
          label: "Metadata Tests",
          type: "doc",
          id: "docs/tests/metadata-tests",
          className: "saasOnly",
        },
        {
          // Incidents are available in DataHub Core as a manual signal on an
          // asset. DataHub Cloud additionally raises them automatically from
          // failing assertions -- that behaviour is documented under
          // DataHub Cloud > Data Quality & Observability. Kept here, unmarked,
          // so self-hosted users can find a feature they already have.
          label: "Incidents",
          type: "doc",
          id: "docs/incidents/incidents",
        },
        {
          label: "Logical Models",
          type: "category",
          link: {
            type: "doc",
            id: "docs/features/feature-guides/logical-models/overview",
          },
          items: [
            {
              label: "Overview",
              type: "doc",
              id: "docs/features/feature-guides/logical-models/overview",
            },
            {
              label: "Centralized Management",
              type: "doc",
              id: "docs/features/feature-guides/logical-models/centralized-management",
              className: "saasOnly",
            },
          ],
        },
        {
          label: "Metrics & Semantic Models",
          type: "doc",
          id: "docs/features/feature-guides/metrics-and-semantic-models",
        },
        {
          label: "Service Catalog",
          type: "doc",
          id: "docs/features/feature-guides/service-catalog",
          className: "saasOnly",
        },
      ],
    },
    {
      label: "AI & Agents",
      type: "category",
      collapsed: true,
      link: {
        type: "generated-index",
        title: "AI & Agents",
        description:
          "Put your metadata to work with AI — conversational discovery, agents, and the context layer that grounds them.",
      },
      items: [
        {
          label: "Ask DataHub",
          type: "category",
          className: "saasOnly",
          link: {
            type: "doc",
            id: "docs/features/feature-guides/ask-datahub",
          },
          items: [
            {
              label: "Plugins",
              type: "category",
              link: {
                type: "doc",
                id: "docs/features/feature-guides/ask-datahub-plugins/overview",
              },
              items: [
                {
                  label: "Snowflake",
                  type: "doc",
                  id: "docs/features/feature-guides/ask-datahub-plugins/snowflake",
                },
                {
                  label: "Databricks",
                  type: "doc",
                  id: "docs/features/feature-guides/ask-datahub-plugins/databricks",
                },
                {
                  label: "BigQuery",
                  type: "doc",
                  id: "docs/features/feature-guides/ask-datahub-plugins/bigquery",
                },
                {
                  label: "dbt Cloud",
                  type: "doc",
                  id: "docs/features/feature-guides/ask-datahub-plugins/dbt",
                },
                {
                  label: "GitHub",
                  type: "doc",
                  id: "docs/features/feature-guides/ask-datahub-plugins/github",
                },
                {
                  label: "Glean",
                  type: "doc",
                  id: "docs/features/feature-guides/ask-datahub-plugins/glean",
                },
              ],
            },
          ],
        },
        {
          label: "Agents",
          type: "doc",
          id: "docs/features/feature-guides/agents",
          className: "saasOnly",
        },
        {
          label: "Analytics Agent",
          type: "doc",
          id: "docs/features/feature-guides/analytics-agent",
        },
        {
          label: "Agent Registry",
          type: "doc",
          id: "docs/features/feature-guides/agent-registry",
          className: "saasOnly",
        },
        {
          label: "MCP Server",
          type: "category",
          link: {
            type: "doc",
            id: "docs/features/feature-guides/mcp",
          },
          items: [
            {
              label: "Scoped MCP Servers",
              type: "doc",
              id: "docs/features/feature-guides/scoped-mcp-servers",
              className: "saasOnly",
            },
          ],
        },
        {
          label: "Agent Context Kit",
          type: "category",
          link: {
            type: "doc",
            id: "docs/dev-guides/agent-context/agent-context",
          },
          items: [
            {
              label: "DataHub Skills",
              type: "doc",
              id: "docs/dev-guides/agent-context/skills",
            },
            {
              label: "Snowflake",
              type: "category",
              collapsible: true,
              collapsed: true,
              items: [
                {
                  label: "Cortex Agents",
                  type: "doc",
                  id: "docs/dev-guides/agent-context/snowflake",
                },
                {
                  label: "Cortex Code",
                  type: "doc",
                  id: "docs/dev-guides/agent-context/snowflake-cortex-code",
                },
              ],
            },
            {
              label: "Google",
              type: "category",
              collapsible: true,
              collapsed: true,
              items: [
                {
                  label: "Gemini CLI",
                  type: "doc",
                  id: "docs/dev-guides/agent-context/gemini-cli",
                },
                {
                  label: "Agent Development Kit (ADK)",
                  type: "doc",
                  id: "docs/dev-guides/agent-context/google-adk",
                },
                {
                  label: "Vertex AI",
                  type: "doc",
                  id: "docs/dev-guides/agent-context/google-vertex-ai",
                },
              ],
            },
            {
              label: "Databricks",
              type: "category",
              collapsible: true,
              collapsed: true,
              items: [
                {
                  label: "Genie Code",
                  type: "doc",
                  id: "docs/dev-guides/agent-context/databricks-genie-code",
                },
                {
                  label: "Agent Bricks",
                  type: "doc",
                  id: "docs/dev-guides/agent-context/databricks-agent-bricks",
                },
              ],
            },
            {
              label: "LangChain",
              type: "doc",
              id: "docs/dev-guides/agent-context/langchain",
            },
            {
              label: "Cursor",
              type: "doc",
              id: "docs/dev-guides/agent-context/cursor",
            },
            {
              label: "Claude",
              type: "doc",
              id: "docs/dev-guides/agent-context/claude",
            },
            {
              label: "Microsoft Copilot Studio",
              type: "doc",
              id: "docs/dev-guides/agent-context/copilot-studio",
            },
          ],
        },
      ],
    },
    {
      label: "Automation & Workflows",
      type: "category",
      collapsed: true,
      link: {
        type: "generated-index",
        title: "Automation & Workflows",
        description:
          "Keep metadata current without manual effort — propagation, source-system sync, and approval workflows.",
      },
      items: [
        {
          label: "Automations",
          type: "category",
          collapsed: true,
          items: [
            {
              label: "Documentation Propagation",
              type: "doc",
              id: "docs/automations/docs-propagation",
            },
            {
              label: "Glossary Term Propagation",
              type: "doc",
              id: "docs/automations/glossary-term-propagation",
            },
            {
              label: "BigQuery Metadata Sync",
              type: "doc",
              id: "docs/automations/bigquery-metadata-sync",
              className: "saasOnly",
            },
            {
              label: "Knowledge Catalog Metadata Sync",
              type: "doc",
              id: "docs/automations/knowledge-catalog-metadata-sync",
              className: "saasOnly",
            },
            {
              label: "Databricks Metadata Sync",
              type: "doc",
              id: "docs/automations/databricks-metadata-sync",
              className: "saasOnly",
            },
            {
              label: "Snowflake Metadata Sync",
              type: "doc",
              id: "docs/automations/snowflake-metadata-sync",
              className: "saasOnly",
            },
            {
              label: "AI Documentation",
              type: "doc",
              id: "docs/automations/ai-docs",
              className: "saasOnly",
            },
          ],
        },
        {
          label: "Workflows",
          type: "category",
          className: "saasOnly",
          collapsed: true,
          items: [
            {
              label: "Data Access Workflows",
              type: "doc",
              id: "docs/managed-datahub/workflows/access-workflows",
              className: "saasOnly",
            },
            {
              label: "Action Workflows",
              type: "doc",
              id: "docs/managed-datahub/workflows/action-workflows",
              className: "saasOnly",
            },
            {
              label: "Workflow Tutorial",
              type: "doc",
              id: "docs/managed-datahub/workflows/action-workflows-tutorial",
              className: "saasOnly",
            },
            {
              label: "Workflow Reference",
              type: "doc",
              id: "docs/managed-datahub/workflows/action-workflows-reference",
              className: "saasOnly",
            },
          ],
        },
      ],
    },
    {
      label: "Collaboration & Workspace",
      type: "category",
      collapsed: true,
      link: {
        type: "generated-index",
        title: "Collaboration & Workspace",
        description:
          "Share knowledge and tailor DataHub to how your team works — home page, documents, and announcements.",
      },
      items: [
        {
          label: "Home Page",
          type: "doc",
          id: "docs/features/feature-guides/custom-home-page",
        },
        {
          label: "Asset Summaries",
          type: "doc",
          id: "docs/features/feature-guides/custom-asset-summaries",
        },
        {
          label: "Posts",
          type: "doc",
          id: "docs/posts",
        },
        {
          label: "Context Documents",
          type: "category",
          link: {
            type: "doc",
            id: "docs/features/feature-guides/context/context-documents",
          },
          items: [
            {
              label: "Import from Notion",
              type: "doc",
              id: "docs/features/feature-guides/context/import-notion",
              className: "saasOnly",
            },
            {
              label: "Import from Confluence",
              type: "doc",
              id: "docs/features/feature-guides/context/import-confluence",
              className: "saasOnly",
            },
            {
              label: "Import from GitHub",
              type: "doc",
              id: "docs/features/feature-guides/context/import-github",
              className: "saasOnly",
            },
          ],
        },
        {
          label: "File Upload & Download",
          type: "doc",
          id: "docs/features/feature-guides/file-upload-download",
        },
        {
          label: "Multi-Language Support",
          type: "doc",
          id: "docs/features/feature-guides/multi-language-support",
        },
      ],
    },
    // DataHub Cloud.
    {
      type: "html",
      value: "<div>DataHub Cloud</div>",
      defaultStyle: true,
    },
    {
      label: "DataHub Cloud Overview",
      type: "doc",
      id: "docs/managed-datahub/managed-datahub-overview",
    },
    {
      label: "Getting Started with DataHub Cloud",
      type: "doc",
      id: "docs/managed-datahub/welcome-acryl",
    },
    {
      label: "Data Quality & Observability",
      type: "category",
      collapsed: true,
      customProps: {
        icon: "🔍",
      },
      link: {
        type: "doc",
        id: "docs/managed-datahub/observe/overview",
      },
      items: [
        {
          label: "Assertions",
          type: "category",
          className: "saasOnly",
          link: { type: "doc", id: "docs/managed-datahub/observe/assertions" },
          items: [
            {
              label: "Overview",
              type: "doc",
              id: "docs/managed-datahub/observe/assertions",
              className: "saasOnly",
            },
            {
              label: "Column Assertions",
              type: "doc",
              id: "docs/managed-datahub/observe/column-assertions",
              className: "saasOnly",
            },
            {
              label: "Custom SQL Assertions",
              type: "doc",
              id: "docs/managed-datahub/observe/custom-sql-assertions",
              className: "saasOnly",
            },
            {
              label: "Freshness Assertions",
              type: "doc",
              id: "docs/managed-datahub/observe/freshness-assertions",
              className: "saasOnly",
            },
            {
              label: "Schema Assertions",
              type: "doc",
              id: "docs/managed-datahub/observe/schema-assertions",
              className: "saasOnly",
            },
            {
              label: "Volume Assertions",
              type: "doc",
              id: "docs/managed-datahub/observe/volume-assertions",
              className: "saasOnly",
            },
            {
              label: "Anomaly Detection ⚡",
              type: "doc",
              id: "docs/managed-datahub/observe/anomaly-detection",
              className: "saasOnly",
            },
            {
              label: "Backfill Assertion History",
              type: "doc",
              id: "docs/managed-datahub/observe/assertion-backfill",
              className: "saasOnly",
            },
            {
              label: "Adding Notes to Assertions",
              type: "doc",
              id: "docs/managed-datahub/observe/assertion-notes",
              className: "saasOnly",
            },
            {
              label: "Assertion Query Attribution",
              type: "doc",
              id: "docs/managed-datahub/observe/assertion-query-attribution",
              className: "saasOnly",
            },
            {
              label: "Open Assertions Specification",
              type: "category",
              link: { type: "doc", id: "docs/assertions/open-assertions-spec" },
              items: [
                {
                  label: "Snowflake",
                  type: "doc",
                  id: "docs/assertions/snowflake/snowflake_dmfs",
                },
              ],
            },
          ],
        },
        {
          label: "Data Contract",
          type: "doc",
          id: "docs/managed-datahub/observe/data-contract",
          className: "saasOnly",
        },
        {
          label: "Data Health Dashboard",
          type: "doc",
          id: "docs/managed-datahub/observe/data-health-dashboard",
          className: "saasOnly",
        },
        {
          label: "Subscriptions & Notifications",
          type: "category",
          className: "saasOnly",
          link: {
            type: "doc",
            id: "docs/managed-datahub/subscription-and-notification",
          },
          items: [
            {
              label: "SMTP Email Notifications",
              type: "doc",
              id: "docs/managed-datahub/smtp-email",
              className: "saasOnly",
            },
          ],
        },
      ],
    },
    {
      label: "Upgrading from DataHub Core to Cloud",
      type: "doc",
      id: "docs/managed-datahub/upgrade_core_to_cloud",
    },
    {
      label: "Configuration",
      type: "category",
      collapsed: true,
      items: [
        {
          "Configure Single Sign-On": [
            {
              type: "doc",
              id: "docs/authentication/guides/sso/initialize-oidc",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/integrations/oidc-sso-integration",
              className: "saasOnly",
            },
          ],
        },
        {
          "Remote Executor": [
            {
              type: "doc",
              id: "docs/managed-datahub/remote-executor/about",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/remote-executor/best-practices",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/remote-executor/monitoring",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/remote-executor/removing-sqs-dependency",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/remote-executor/bundling-additional-connectors",
              className: "saasOnly",
            },
          ],
        },
        {
          "Operator Guides": [
            {
              type: "doc",
              id: "docs/managed-datahub/operator-guide/setting-up-events-api-on-aws-eventbridge",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/integrations/aws-privatelink",
              className: "saasOnly",
            },
          ],
        },
      ],
    },
    {
      label: "Cloud APIs",
      type: "category",
      collapsed: true,
      items: [
        {
          type: "doc",
          id: "docs/managed-datahub/datahub-api/entity-events-api",
          className: "saasOnly",
        },
        {
          "GraphQL API": [
            "docs/managed-datahub/datahub-api/graphql-api/getting-started",
          ],
        },
      ],
    },
    {
      label: "Apps & Notifications",
      type: "category",
      collapsed: true,
      items: [
        {
          Slack: [
            {
              type: "doc",
              id: "docs/managed-datahub/slack/saas-slack-app",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/slack/saas-slack-setup",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/slack/saas-slack-troubleshoot",
              className: "saasOnly",
            },
          ],
        },
        {
          Teams: [
            {
              type: "doc",
              id: "docs/managed-datahub/teams/saas-teams-app",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/teams/saas-teams-setup",
              className: "saasOnly",
            },
          ],
        },
        {
          label: "Chrome Extension",
          type: "doc",
          id: "docs/managed-datahub/chrome-extension",
        },
      ],
    },
    {
      label: "Change Proposals",
      type: "doc",
      id: "docs/managed-datahub/change-proposals",
      className: "saasOnly",
    },
    {
      "DataHub Cloud Release History": [
        "docs/managed-datahub/release-notes/v_2_2_0",
        "docs/managed-datahub/release-notes/v_2_1_0",
        "docs/managed-datahub/release-notes/v_2_0_0",
        "docs/managed-datahub/release-notes/v_1_1_0",
        "docs/managed-datahub/release-notes/v_1_0_0",
        "docs/managed-datahub/release-notes/v_0_3_17",
        "docs/managed-datahub/release-notes/v_0_3_16",
        "docs/managed-datahub/release-notes/v_0_3_15",
        "docs/managed-datahub/release-notes/v_0_3_14",
        "docs/managed-datahub/release-notes/v_0_3_13",
        "docs/managed-datahub/release-notes/v_0_3_12",
        "docs/managed-datahub/release-notes/v_0_3_11",
        "docs/managed-datahub/release-notes/v_0_3_10",
        "docs/managed-datahub/release-notes/v_0_3_9",
        "docs/managed-datahub/release-notes/v_0_3_8",
        "docs/managed-datahub/release-notes/v_0_3_7",
        "docs/managed-datahub/release-notes/v_0_3_6",
        "docs/managed-datahub/release-notes/v_0_3_5",
        "docs/managed-datahub/release-notes/v_0_3_4",
        "docs/managed-datahub/release-notes/v_0_3_3",
        "docs/managed-datahub/release-notes/v_0_3_2",
        "docs/managed-datahub/release-notes/v_0_3_1",
        "docs/managed-datahub/release-notes/v_0_2_16",
        "docs/managed-datahub/release-notes/v_0_2_15",
        "docs/managed-datahub/release-notes/v_0_2_14",
        "docs/managed-datahub/release-notes/v_0_2_13",
        "docs/managed-datahub/release-notes/v_0_2_12",
        "docs/managed-datahub/release-notes/v_0_2_11",
        "docs/managed-datahub/release-notes/v_0_2_10",
        "docs/managed-datahub/release-notes/v_0_2_9",
        "docs/managed-datahub/release-notes/v_0_2_8",
        "docs/managed-datahub/release-notes/v_0_2_7",
        "docs/managed-datahub/release-notes/v_0_2_6",
        "docs/managed-datahub/release-notes/v_0_2_5",
        "docs/managed-datahub/release-notes/v_0_2_4",
        "docs/managed-datahub/release-notes/v_0_2_3",
        "docs/managed-datahub/release-notes/v_0_2_2",
        "docs/managed-datahub/release-notes/v_0_2_1",
        "docs/managed-datahub/release-notes/v_0_2_0",
        "docs/managed-datahub/release-notes/v_0_1_73",
        "docs/managed-datahub/release-notes/v_0_1_72",
        "docs/managed-datahub/release-notes/v_0_1_70",
        "docs/managed-datahub/release-notes/v_0_1_69",
      ],
    },
    // Integrations.
    {
      type: "html",
      value: "<div>Integrations</div>",
      defaultStyle: true,
    },
    {
      type: "category",
      link: {
        type: "doc",
        id: "metadata-ingestion/README",
      },
      label: "Overview",
      items: [
        {
          type: "doc",
          label: "Recipe",
          id: "metadata-ingestion/recipe_overview",
        },
        {
          type: "category",
          label: "Sinks",
          link: { type: "doc", id: "metadata-ingestion/sink_overview" },
          items: [
            {
              type: "autogenerated",
              dirName: "metadata-ingestion/sink_docs",
            },
          ],
        },
        {
          type: "category",
          label: "Transformers",
          link: {
            type: "doc",
            id: "metadata-ingestion/docs/transformer/intro",
          },
          items: [
            "metadata-ingestion/docs/transformer/dataset_transformer",
            "metadata-ingestion/docs/transformer/universal_transformers",
          ],
        },
      ],
    },
    {
      "Quickstart Guides": [
        {
          BigQuery: [
            "docs/quick-ingestion-guides/bigquery/overview",
            "docs/quick-ingestion-guides/bigquery/setup",
            "docs/quick-ingestion-guides/bigquery/configuration",
          ],
        },
        {
          Redshift: [
            "docs/quick-ingestion-guides/redshift/overview",
            "docs/quick-ingestion-guides/redshift/setup",
            "docs/quick-ingestion-guides/redshift/configuration",
          ],
        },
        {
          Snowflake: [
            "docs/quick-ingestion-guides/snowflake/overview",
            "docs/quick-ingestion-guides/snowflake/setup",
            "docs/quick-ingestion-guides/snowflake/configuration",
          ],
        },
        {
          Tableau: [
            "docs/quick-ingestion-guides/tableau/overview",
            "docs/quick-ingestion-guides/tableau/setup",
            "docs/quick-ingestion-guides/tableau/configuration",
          ],
        },
        {
          "Power BI": [
            "docs/quick-ingestion-guides/powerbi/overview",
            "docs/quick-ingestion-guides/powerbi/setup",
            "docs/quick-ingestion-guides/powerbi/configuration",
          ],
        },
        {
          Looker: [
            "docs/quick-ingestion-guides/looker/overview",
            "docs/quick-ingestion-guides/looker/setup",
            "docs/quick-ingestion-guides/looker/configuration",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Sources",
      link: { type: "doc", id: "metadata-ingestion/source_overview" },
      items: buildSourceGroups(),
    },
    {
      type: "category",
      label: "Running Ingestion",
      collapsed: true,
      items: [
        {
          type: "doc",
          label: "UI Ingestion",
          id: "docs/ui-ingestion",
        },
        {
          type: "doc",
          label: "CLI Ingestion",
          id: "metadata-ingestion/cli-ingestion",
        },
        {
          type: "doc",
          label: "Ingestion Security",
          id: "docs/metadata-ingestion-security",
        },
        {
          "Scheduling Ingestion": [
            "metadata-ingestion/schedule_docs/intro",
            "metadata-ingestion/schedule_docs/cron",
            "metadata-ingestion/schedule_docs/airflow",
            "metadata-ingestion/schedule_docs/kubernetes",
          ],
        },
      ],
    },
    {
      "Advanced Ingestion": [
        "metadata-ingestion/datahub-skills",
        "docs/platform-instances",
        "docs/lineage/sql_parsing",
        "metadata-ingestion/docs/dev_guides/stateful",
        "metadata-ingestion/docs/dev_guides/classification",
        "metadata-ingestion/docs/dev_guides/add_stateful_ingestion_to_source",
        "metadata-ingestion/docs/dev_guides/sql_profiles",
        "metadata-ingestion/docs/dev_guides/profiling_ingestions",
        "metadata-ingestion/docs/dev_guides/lineage_urn_casing",
        "docs/iceberg-catalog",
      ],
    },
    // APIs & SDKs.
    {
      type: "html",
      value: "<div>API & SDKs</div>",
      defaultStyle: true,
    },
    {
      type: "doc",
      id: "docs/api/datahub-apis",
      label: "Overview",
    },
    {
      type: "category",
      label: "Open Source DataHub Metadata Standard",
      link: { type: "doc", id: "docs/metadata-standards" },
      collapsed: false,
      items: [
        {
          type: "doc",
          label: "The Metadata Model",
          id: "docs/modeling/metadata-model",
        },
        {
          type: "doc",
          label: "Core Metadata Events",
          id: "docs/what/mxe",
        },
        {
          type: "category",
          label: "Entity Reference",
          items: [
            {
              type: "autogenerated",
              dirName: "docs/generated/metamodel/entities",
            },
          ],
        },
      ],
    },
    {
      type: "category",
      label: "APIs",
      items: [
        {
          "GraphQL API": [
            {
              label: "Overview",
              type: "doc",
              id: "docs/api/graphql/overview",
            },
            {
              Reference: [
                {
                  type: "doc",
                  label: "Queries",
                  id: "graphql/queries",
                },
                {
                  type: "doc",
                  label: "Mutations",
                  id: "graphql/mutations",
                },
                {
                  type: "doc",
                  label: "Objects",
                  id: "graphql/objects",
                },
                {
                  type: "doc",
                  label: "Inputs",
                  id: "graphql/inputObjects",
                },
                {
                  type: "doc",
                  label: "Interfaces",
                  id: "graphql/interfaces",
                },
                {
                  type: "doc",
                  label: "Unions",
                  id: "graphql/unions",
                },
                {
                  type: "doc",
                  label: "Enums",
                  id: "graphql/enums",
                },
                {
                  type: "doc",
                  label: "Scalars",
                  id: "graphql/scalars",
                },
              ],
            },
            {
              Guides: [
                {
                  type: "doc",
                  label: "How To Set Up GraphQL",
                  id: "docs/api/graphql/how-to-set-up-graphql",
                },
                {
                  type: "doc",
                  label: "Getting Started With GraphQL",
                  id: "docs/api/graphql/getting-started",
                },
                {
                  type: "doc",
                  label: "GraphQL Best Practices",
                  id: "docs/api/graphql/graphql-best-practices",
                },
                {
                  type: "doc",
                  label: "Access Token Management",
                  id: "docs/api/graphql/token-management",
                },
              ],
            },
          ],
        },
        {
          OpenAPI: [
            {
              type: "doc",
              label: "OpenAPI",
              id: "docs/api/openapi/openapi-usage-guide",
            },
          ],
        },
        {
          "Rest.li API": [
            {
              type: "doc",
              label: "Rest.li API Guide",
              id: "docs/api/restli/restli-overview",
            },
            {
              type: "doc",
              label: "Restore Indices",
              id: "docs/api/restli/restore-indices",
            },
            {
              type: "doc",
              label: "Get Index Sizes",
              id: "docs/api/restli/get-index-sizes",
            },
            {
              type: "doc",
              label: "Truncate Timeseries Aspect",
              id: "docs/api/restli/truncate-time-series-aspect",
            },
            {
              type: "doc",
              label: "Get ElasticSearch Task Status Endpoint",
              id: "docs/api/restli/get-elastic-task-status",
            },
            {
              type: "doc",
              label: "Evaluate Tests",
              id: "docs/api/restli/evaluate-tests",
            },
            {
              type: "doc",
              label: "Aspect Versioning and Rest.li Modeling",
              id: "docs/advanced/aspect-versioning",
            },
          ],
        },
        {
          type: "doc",
          label: "Timeline API",
          id: "docs/dev-guides/timeline",
        },
      ],
    },
    {
      type: "category",
      label: "Python SDK",
      items: [
        "metadata-ingestion/as-a-library",
        {
          type: "category",
          label: "SDK Reference",
          items: [
            {
              type: "category",
              label: "Builder",
              items: [{ type: "autogenerated", dirName: "python-sdk/builder" }],
            },
            {
              type: "category",
              label: "Clients",
              items: [{ type: "autogenerated", dirName: "python-sdk/clients" }],
            },
            {
              type: "category",
              label: "SDK V2",
              items: [{ type: "autogenerated", dirName: "python-sdk/sdk-v2" }],
            },
            "python-sdk/models",
            "python-sdk/urns",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Java SDK",
      items: [
        {
          type: "doc",
          label: "Java SDK V1 (Legacy)",
          id: "metadata-integration/java/as-a-library",
        },
        {
          type: "category",
          label: "SDK V2",
          link: {
            type: "doc",
            id: "metadata-integration/java/as-a-library-v2",
          },
          items: [
            "metadata-integration/java/docs/sdk-v2/getting-started",
            "metadata-integration/java/docs/sdk-v2/client",
            "metadata-integration/java/docs/sdk-v2/entities-overview",
            {
              type: "category",
              label: "Entity Guides",
              items: [
                "metadata-integration/java/docs/sdk-v2/dataset-entity",
                "metadata-integration/java/docs/sdk-v2/chart-entity",
                "metadata-integration/java/docs/sdk-v2/dashboard-entity",
                "metadata-integration/java/docs/sdk-v2/container-entity",
                "metadata-integration/java/docs/sdk-v2/dataflow-entity",
                "metadata-integration/java/docs/sdk-v2/datajob-entity",
                "metadata-integration/java/docs/sdk-v2/mlmodel-entity",
                "metadata-integration/java/docs/sdk-v2/mlmodelgroup-entity",
              ],
            },
            "metadata-integration/java/docs/sdk-v2/patch-operations",
            "metadata-integration/java/docs/sdk-v2/migration-from-v1",
            "metadata-integration/java/docs/sdk-v2/design-principles",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "DataHub CLI",
      link: { type: "doc", id: "docs/cli" },
      items: [
        { type: "doc", id: "docs/cli-commands/search", label: "search" },
        { type: "doc", id: "docs/cli-commands/graphql", label: "graphql" },
        { type: "doc", id: "docs/cli-commands/dataset", label: "dataset" },
        { type: "doc", id: "docs/cli-commands/datapack", label: "datapack" },
        {
          type: "doc",
          id: "docs/cli-commands/evals",
          label: "evals",
          className: "saasOnly",
        },
        { type: "doc", id: "docs/datahub_lite", label: "lite" },
      ],
    },
    {
      type: "category",
      label: "DataHub Actions",
      link: { type: "doc", id: "docs/act-on-metadata" },
      items: [
        "docs/actions/README",
        "docs/actions/quickstart",
        "docs/actions/concepts",
        {
          Sources: [
            {
              type: "autogenerated",
              dirName: "docs/actions/sources",
            },
          ],
        },
        {
          Events: [
            {
              type: "autogenerated",
              dirName: "docs/actions/events",
            },
          ],
        },
        {
          Actions: [
            {
              type: "autogenerated",
              dirName: "docs/actions/actions",
            },
          ],
        },
        {
          Guides: [
            {
              type: "autogenerated",
              dirName: "docs/actions/guides",
            },
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Tutorials",
      collapsed: true,
      items: [
        {
          "Core Objects": [
            "docs/api/tutorials/datasets",
            "docs/api/tutorials/container",
            "docs/api/tutorials/dashboard-chart",
            "docs/api/tutorials/dataflow-datajob",
            "docs/api/tutorials/mlmodel-mlmodelgroup",
            "docs/api/tutorials/semantic-models",
            "docs/api/tutorials/applications",
            "docs/api/tutorials/agent-registry",
            "docs/api/tutorials/service-catalog",
          ],
        },
        {
          "Metadata & Governance": [
            "docs/api/tutorials/descriptions",
            "docs/api/tutorials/documents",
            "docs/api/tutorials/custom-properties",
            "docs/api/tutorials/structured-properties",
            "docs/api/tutorials/tags",
            "docs/api/tutorials/terms",
            "docs/api/tutorials/owners",
            "docs/api/tutorials/domains",
            "docs/api/tutorials/deprecation",
          ],
        },
        "docs/api/tutorials/lineage",
        {
          "Quality & Contracts": [
            "docs/api/tutorials/assertions",
            "docs/api/tutorials/custom-assertions",
            "docs/api/tutorials/sdk/bulk-assertions-sdk",
            "docs/api/tutorials/incidents",
            "docs/api/tutorials/operations",
            "docs/api/tutorials/data-contracts",
            "docs/api/tutorials/forms",
            "docs/api/tutorials/subscriptions",
          ],
        },
        {
          "AI & ML": [
            {
              type: "doc",
              id: "docs/api/tutorials/ml",
              label: "AI/ML Integration",
            },
            {
              type: "doc",
              id: "docs/api/tutorials/ml_feature_store",
              label: "Feature Store",
            },
          ],
        },
        {
          Advanced: [
            {
              type: "doc",
              id: "docs/advanced/patch",
              label: "Patch",
            },
            "docs/api/tutorials/sdk/search_client",
          ],
        },
      ],
    },
    // Admin.
    {
      type: "html",
      value: "<div>Admin</div>",
      defaultStyle: true,
    },
    {
      Authentication: [
        "docs/authentication/README",
        "docs/authentication/concepts",
        "docs/authentication/changing-default-credentials",
        "docs/authentication/guides/add-users",
        {
          "Frontend Authentication": [
            "docs/authentication/guides/jaas",
            "docs/authentication/guides/sso/initialize-oidc",
            "docs/authentication/guides/sso/configure-oidc-react",
            "docs/authentication/guides/sso/configure-oidc-behind-proxy",
          ],
        },
        {
          "SCIM Provisioning": [
            {
              type: "doc",
              id: "docs/managed-datahub/configuring-identity-provisioning-with-ms-entra",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/configuring-identity-provisioning-with-okta",
              className: "saasOnly",
            },
          ],
        },
        "docs/authentication/introducing-metadata-service-authentication",
        "docs/authentication/personal-access-tokens",
        "docs/authentication/external-oauth-providers",
      ],
    },
    {
      Authorization: [
        "docs/authorization/README",
        "docs/authorization/roles",
        "docs/authorization/policies",
        "docs/authorization/groups",
      ],
    },
    {
      Operations: [
        "docs/how/delete-metadata",
        "docs/how/configuring-authorization-with-apache-ranger",
        "docs/how-to/semantic-search-configuration",
        "docs/how/backup-datahub",
        "docs/how/restore-indices",
        "docs/how/load-indices",
        "docs/advanced/db-retention",
        "docs/advanced/monitoring",
        "docs/deploy/telemetry",
        "docs/how/kafka-config",
        "docs/how/configure-cdc",
        "docs/how/jattach-guide",
      ],
    },
    // Deployment.
    {
      type: "html",
      value: "<div>Deployment</div>",
      defaultStyle: true,
    },
    {
      type: "category",
      label: "Deployment Guides",
      link: {
        type: "generated-index",
        title: "Deployment Guides",
        description:
          "Learn how to deploy DataHub to your environment, set up authentication, manage upgrades, and more.",
      },
      items: [
        "docs/deploy/aws",
        "docs/deploy/gcp",
        "docs/deploy/azure",
        "docker/README",
        "docs/deploy/kubernetes",
      ],
    },
    {
      type: "category",
      label: "Advanced Configuration",
      items: [
        "docs/deploy/confluent-cloud",
        "docs/deploy/gms-rate-limiting",
        "docs/deploy/gms-entity-graph-cache",
        "docs/deploy/primary-storage-read-pool",
        "docs/deploy/environment-vars",
        "docs/how/extract-container-logs",
      ],
    },
    // Developers.
    {
      type: "html",
      value: "<div>Developers</div>",
      defaultStyle: true,
    },
    {
      Architecture: [
        "docs/architecture/architecture",
        "docs/components",
        "docs/architecture/metadata-ingestion",
        "docs/architecture/metadata-serving",
        "docs/architecture/docker-containers",
      ],
    },
    {
      "Developing on DataHub": [
        "docs/developers",
        "docs/developers/java-sdk-v2-design",
        "docs/docker/development",
        "metadata-ingestion/developing",
        "docs/api/graphql/graphql-endpoint-development",
        {
          Modules: [
            "datahub-web-react/README",
            "datahub-frontend/README",
            "datahub-graphql-core/README",
            "metadata-service/README",
            "metadata-jobs/mae-consumer-job/README",
            "metadata-jobs/mce-consumer-job/README",
          ],
        },
        {
          Troubleshooting: [
            "docs/troubleshooting/quickstart",
            "docs/troubleshooting/build",
            "docs/troubleshooting/general",
          ],
        },
      ],
    },
    {
      "Extending DataHub": [
        {
          "Metadata Model": [
            "docs/advanced/mcp-mcl",
            "docs/advanced/writing-mcps",
            "docs/modeling/extending-the-metadata-model",
            "docs/advanced/bootstrap-mcps",
            "docs/advanced/field-path-spec-v2",
          ],
        },
        {
          "Building Connectors": [
            "metadata-ingestion/adding-source",
            "docs/how/add-custom-ingestion-source",
            "docs/how/add-custom-data-platform",
            "metadata-ingestion/docs/dev_guides/reporting_telemetry",
            "docs/docker/bundled-ingestion-venvs",
            "docs/docker/ingestion-executor-security",
          ],
        },
        {
          "Search & Storage": [
            {
              "Semantic Search": [
                "docs/dev-guides/semantic-search/README",
                "docs/dev-guides/semantic-search/ARCHITECTURE",
                "docs/dev-guides/semantic-search/CONFIGURATION",
                "docs/dev-guides/semantic-search/SWITCHING_PROVIDERS",
              ],
            },
            "docs/how/migrating-graph-service-implementation",
            "docs/how/migrating-elasticsearch-opensearch",
            "docs/browseV2/browse-paths-v2",
            "docs/advanced/browse-paths-upgrade",
          ],
        },
        {
          "Operations & Tooling": [
            "docs/advanced/api-tracing",
            "docs/advanced/micrometer-best-practices",
            "datahub-web-react/src/app/analytics/README",
            // "smoke-test/test_resources/analytics_backfill/README",
            "docker/datahub-upgrade/README",
            "docs/plugins",
          ],
        },
      ],
    },
    // Community.
    {
      type: "html",
      value: "<div>Community</div>",
      defaultStyle: true,
    },
    {
      label: "Community",
      type: "category",
      collapsed: true,
      link: {
        type: "generated-index",
        title: "Community",
        description: "Learn about DataHub community.",
      },
      items: [
        "docs/slack",
        { type: "doc", label: "Otto (Community Assistant)", id: "docs/otto" },
        "docs/townhalls",
        //        "docs/townhall-history",
        "docs/CODE_OF_CONDUCT",
        "docs/CONTRIBUTING",
        "docs/links",
        "docs/rfc",
        {
          type: "category",
          label: "RFCs",
          link: { type: "doc", id: "docs/rfcs/README" },
          items: [],
        },
        "SECURITY",
      ],
    },
    {
      "Release History": ["releases", "docs/how/updating-datahub"],
    },

    // "Candidates for Deprecation": [
    // "README",
    // "docs/roadmap",
    // "docs/advanced/backfilling",
    //"docs/advanced/derived-aspects",
    //"docs/advanced/entity-hierarchy",
    //"docs/advanced/partial-update",
    //"docs/advanced/pdl-best-practices",
    //"docs/introducing-metadata-service-authentication"
    //"metadata-models-custom/README"
    //"metadata-ingestion/examples/transforms/README"
    //"docs/what/graph",
    //"docs/what/search-index",
    //"docs/how/add-new-aspect",
    //"docs/how/build-metadata-service",
    //"docs/how/graph-onboarding",
    //"docs/demo/graph-onboarding",
    //"datahub-actions/README",
    //"datahub-actions/src/datahub_actions/plugin/action/tag/README",
    //"datahub-actions/src/datahub_actions/plugin/action/term/README",
    //"metadata-integration/java/spark-lineage/README",
    // "metadata-integration/java/acryl-spark-lineage/README.md
    // "metadata-integration/java/openlineage-converter/README"
    //"metadata-ingestion-modules/airflow-plugin/README"
    //"metadata-ingestion-modules/dagster-plugin/README"
    //"metadata-ingestion-modules/prefect-plugin/README"
    //"metadata-ingestion-modules/gx-plugin/README"
    // "metadata-ingestion/schedule_docs/datahub", // we can delete this
    // TODO: change the titles of these, removing the "What is..." portion from the sidebar"
    // "docs/what/entity",
    // "docs/what/aspect",
    // "docs/what/urn",
    // "docs/what/relationship",
    // "docs/advanced/high-cardinality",
    // "docs/what/search-document",
    // "docs/what/snapshot",
    // "docs/what/delta",
    // - "docker/datahub-frontend/README",
    // - "docker/datahub-gms/README",
    // - "docker/datahub-mae-consumer/README",
    // - "docker/datahub-mce-consumer/README",
    // - "docker/datahub-ingestion/README",
    // - "docker/elasticsearch-setup/README",
    // - "docker/ingestion/README",
    // - "docker/mariadb/README",
    // - "docker/mysql/README",
    // - "docker/neo4j/README",
    // - "docker/postgres/README",
    // - "perf-test/README",
    // "metadata-jobs/README",
    // "docs/how/add-user-data",
    // "docs/_feature-guide-template"
    // "docs/_api-guide-template"
    // - "metadata-service/services/README"
    // "metadata-ingestion/examples/structured_properties/README"
    // "smoke-test/tests/openapi/README"
    // "docs/SECURITY_STANCE"
    // "metadata-integration/java/datahub-schematron/README"
    // ],
  ],
};
