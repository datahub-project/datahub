// note: to handle errors where you don't want a markdown file in the sidebar, add it as a comment.
// this will fix errors like `Error: File not accounted for in sidebar: ...`
module.exports = {
  overviewSidebar: [
    // Getting Started.
    {
      type: "html",
      value: "<div>Getting Started</div>",
      defaultStyle: true,
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
          label: "Quickstart",
          id: "docs/quickstart",
        },
        {
          type: "link",
          label: "Demo",
          href: "https://demo.datahubproject.io/",
        },
        {
          type: "link",
          label: "Adoption Stories",
          href: "/adoption-stories",
        },
      ],
    },
    {
      type: "category",
      label: "Features",
      link: {
        type: "generated-index",
        title: "Feature Guides",
        description: "Learn about the features of DataHub.",
      },
      items: [
        // "docs/how/ui-tabs-guide",
        {
          label: "Assertions",
          type: "category",
          link: { type: "doc", id: "docs/managed-datahub/observe/assertions" },
          items: [
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
          label: "Automations",
          type: "category",
          collapsed: false,
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
              label: "Snowflake Tag Sync",
              type: "doc",
              id: "docs/automations/snowflake-tag-propagation",
              className: "saasOnly",
            },
            {
              label: "AI Classification",
              type: "doc",
              id: "docs/automations/ai-term-suggestion",
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
          label: "Business Attributes",
          type: "doc",
          id: "docs/businessattributes",
        },
        {
          label: "Business Glossary",
          type: "doc",
          id: "docs/glossary/business-glossary",
        },
        {
          label: "Compliance Forms",
          type: "category",
          collapsed: true,
          items: [
            {
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
          ],
        },
        {
          label: "Data Contract",
          type: "doc",
          id: "docs/managed-datahub/observe/data-contract",
        },
        {
          label: "Data Products",
          type: "doc",
          id: "docs/dataproducts",
        },
        {
          label: "Dataset Usage and Query History",
          type: "doc",
          id: "docs/features/dataset-usage-and-query-history",
        },
        {
          label: "Domains",
          type: "doc",
          id: "docs/domains",
        },
        {
          label: "Incidents",
          type: "doc",
          id: "docs/incidents/incidents",
        },
        {
          label: "Ingestion",
          type: "doc",
          id: "docs/ui-ingestion",
        },
        {
          label: "Lineage",
          type: "category",
          link: {
            type: "doc",
            id: "docs/generated/lineage/lineage-feature-guide",
          },
          items: [
            {
              label: "Lineage Impact analysis",
              type: "doc",
              id: "docs/act-on-metadata/impact-analysis",
            },
            {
              label: "Managing Lineage via UI",
              type: "doc",
              id: "docs/features/feature-guides/ui-lineage",
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
          label: "Posts",
          type: "doc",
          id: "docs/posts",
        },
        {
          label: "Properties",
          type: "category",
          collapsed: true,
          items: [
            {
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
          label: "Schema history",
          type: "doc",
          id: "docs/schema-history",
        },
        {
          label: "Search",
          type: "doc",
          id: "docs/how/search",
        },
        {
          label: "Sync Status",
          type: "doc",
          id: "docs/sync-status",
        },
        {
          label: "Tags",
          type: "doc",
          id: "docs/tags",
        },
      ],
    },
    {
      label: "DataHub Cloud",
      type: "category",
      collapsed: true,
      link: {
        type: "doc",
        id: "docs/managed-datahub/managed-datahub-overview",
      },
      items: [
        "docs/managed-datahub/welcome-acryl",
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
          "DataHub API": [
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
          Slack: [
            {
              type: "doc",
              id: "docs/managed-datahub/slack/saas-slack-setup",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/slack/saas-slack-app",
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
          "Operator Guides": [
            {
              type: "doc",
              id: "docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor",
              className: "saasOnly",
            },
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
        {
          type: "doc",
          id: "docs/managed-datahub/approval-workflows",
          className: "saasOnly",
        },
        {
          type: "doc",
          id: "docs/managed-datahub/chrome-extension",
        },
        {
          type: "doc",
          id: "docs/managed-datahub/subscription-and-notification",
          className: "saasOnly",
        },
        {
          "DataHub Cloud Release History": [
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
          items: ["metadata-ingestion/docs/transformer/dataset_transformer"],
        },
      ],
    },
    {
      "Quickstart Guides": [
        "metadata-ingestion/cli-ingestion",
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
          PowerBI: [
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
      items: [
        // collapse these; add push-based at top
        {
          type: "doc",
          id: "docs/lineage/airflow",
          label: "Airflow",
        },
        {
          type: "doc",
          id: "docs/lineage/dagster",
          label: "Dagster",
        },
        {
          type: "doc",
          id: "docs/lineage/prefect",
          label: "Prefect",
        },
        {
          type: "doc",
          id: "metadata-integration/java/acryl-spark-lineage/README",
          label: "Spark",
        },
        //"docker/airflow/local_airflow",
        "metadata-ingestion/integration_docs/great-expectations",
        "metadata-integration/java/datahub-protobuf/README",
        //"metadata-integration/java/spark-lineage-legacy/README",
        //"metadata-ingestion/source-docs-template",
        {
          type: "autogenerated",
          dirName: "docs/generated/ingestion/sources", // '.' means the current docs folder
        },
      ],
    },
    {
      "Advanced Guides": [
        {
          "Scheduling Ingestion": [
            "metadata-ingestion/schedule_docs/intro",
            "metadata-ingestion/schedule_docs/cron",
            "metadata-ingestion/schedule_docs/airflow",
            "metadata-ingestion/schedule_docs/kubernetes",
          ],
        },

        "docs/platform-instances",
        "docs/lineage/sql_parsing",
        "metadata-ingestion/docs/dev_guides/stateful",
        "metadata-ingestion/docs/dev_guides/classification",
        "metadata-ingestion/docs/dev_guides/add_stateful_ingestion_to_source",
        "metadata-ingestion/docs/dev_guides/sql_profiles",
        "metadata-ingestion/docs/dev_guides/profiling_ingestions",
      ],
    },
    // APIs & SDKs.
    {
      type: "html",
      value: "<div>API & SDKs</div>",
      defaultStyle: true,
    },
    {
      "DataHub's Open Metadata Standard": [
        "docs/modeling/metadata-model",
        "docs/what/mxe",
        {
          Entities: [
            {
              type: "autogenerated",
              dirName: "docs/generated/metamodel/entities", // '.' means the current docs folder
            },
          ],
        },
      ],
    },
    "docs/what-is-datahub/datahub-concepts",
    {
      type: "category",
      label: "Metadata Standards",
      link: { type: "doc", id: "docs/metadata-standards" },
      items: [
        {
          type: "doc",
          id: "docs/iceberg-catalog",
        },
        {
          type: "doc",
          id: "docs/lineage/openlineage",
          label: "OpenLineage",
        },
      ],
    },
    {
      type: "doc",
      id: "docs/api/datahub-apis",
    },
    {
      type: "category",
      label: "API",
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
        "docs/dev-guides/timeline",
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
      ],
    },
    {
      type: "category",
      label: "Python SDK",
      items: [
        "metadata-ingestion/as-a-library",
        {
          "Python SDK Reference": [
            {
              type: "autogenerated",
              dirName: "python-sdk",
            },
          ],
        },
      ],
    },
    {
      type: "doc",
      label: "Java SDK",
      id: "metadata-integration/java/as-a-library",
    },
    {
      type: "category",
      label: "DataHub CLI",
      link: { type: "doc", id: "docs/cli" },
      items: ["docs/datahub_lite"],
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
      "API & SDK Guides": [
        "docs/api/tutorials/datasets",
        "docs/api/tutorials/deprecation",
        "docs/api/tutorials/descriptions",
        "docs/api/tutorials/custom-properties",
        "docs/api/tutorials/assertions",
        "docs/api/tutorials/custom-assertions",
        "docs/api/tutorials/incidents",
        "docs/api/tutorials/operations",
        "docs/api/tutorials/data-contracts",
        "docs/api/tutorials/domains",
        "docs/api/tutorials/forms",
        "docs/api/tutorials/lineage",
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
        "docs/api/tutorials/owners",
        "docs/api/tutorials/structured-properties",
        "docs/api/tutorials/tags",
        "docs/api/tutorials/terms",
        {
          type: "doc",
          id: "docs/advanced/patch",
          label: "Patch",
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
        "docs/authentication/introducing-metadata-service-authentication",
        "docs/authentication/personal-access-tokens",
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
      "Advanced Guides": [
        "docs/how/delete-metadata",
        "docs/how/configuring-authorization-with-apache-ranger",
        {
          "SCIM Provisioning": [
            "docs/managed-datahub/configuring-identity-provisioning-with-ms-entra",
            "docs/managed-datahub/configuring-identity-provisioning-with-okta",
          ],
        },
        "docs/how/backup-datahub",
        "docs/how/restore-indices",
        "docs/advanced/db-retention",
        "docs/advanced/monitoring",
        "docs/deploy/telemetry",
        "docs/how/kafka-config",
        "docs/advanced/no-code-upgrade",
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
      label: "Advanced Guides",
      items: [
        "docs/deploy/confluent-cloud",
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
      "Advanced Guides": [
        "docs/advanced/mcp-mcl",
        "docs/advanced/writing-mcps",
        "docs/modeling/extending-the-metadata-model",
        "docs/advanced/no-code-modeling",
        "docs/advanced/api-tracing",
        "datahub-web-react/src/app/analytics/README",
        "docker/datahub-upgrade/README",
        "metadata-ingestion/adding-source",
        "docs/how/add-custom-ingestion-source",
        "docs/how/add-custom-data-platform",
        "docs/how/migrating-graph-service-implementation",
        "docs/advanced/field-path-spec-v2",
        "docs/advanced/browse-paths-upgrade",
        "docs/browseV2/browse-paths-v2",
        "metadata-ingestion/docs/dev_guides/reporting_telemetry",
        "docs/plugins",
        "docs/advanced/bootstrap-mcps",
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
        "docs/townhalls",
        //        "docs/townhall-history",
        "docs/CODE_OF_CONDUCT",
        "docs/CONTRIBUTING",
        "docs/links",
        "docs/rfc",
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
    // - "docker/kafka-setup/README",
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
