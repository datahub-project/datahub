// note: to handle errors where you don't want a markdown file in the sidebar, add it as a comment.
// this will fix errors like `Error: File not accounted for in sidebar: ...`
module.exports = {
  // users
  // architects
  // modelers
  // developers
  // operators

  overviewSidebar: [
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
        "docs/what-is-datahub/datahub-concepts",
      ],
    },
    {
      type: "html",
      value: "<div>Features</div>",
      defaultStyle: true,
    },
    {
      type: "category",
      label: "Data Discovery",
      items: [
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
        "docs/features/feature-guides/documentation-forms",
        {
          label: "Domains",
          type: "doc",
          id: "docs/domains",
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
          label: "Properties",
          type: "doc",
          id: "docs/features/feature-guides/properties",
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
          label: "Tags",
          type: "doc",
          id: "docs/tags",
        },
        "docs/features/dataset-usage-and-query-history",
      ],
    },
    {
      type: "category",
      label: "Data Governance",
      items: [
        "docs/approval-workflows",
        "docs/chrome-extension",
        {
          label: "Data Contract",
          type: "doc",
          id: "docs/observe/data-contract",
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
        "docs/posts",
      ],
    },
    {
      type: "category",
      label: "Data Quality",
      items: [
        {
          label: "Assertions",
          type: "category",
          link: { type: "doc", id: "docs/observe/assertions" },
          items: [
            {
              label: "Column Assertions",
              type: "doc",
              id: "docs/observe/column-assertions",
              className: "saasOnly",
            },
            {
              label: "Custom SQL Assertions",
              type: "doc",
              id: "docs/observe/custom-sql-assertions",
              className: "saasOnly",
            },
            {
              label: "Freshness Assertions",
              type: "doc",
              id: "docs/observe/freshness-assertions",
              className: "saasOnly",
            },
            {
              label: "Schema Assertions",
              type: "doc",
              id: "docs/observe/schema-assertions",
              className: "saasOnly",
            },
            {
              label: "Volume Assertions",
              type: "doc",
              id: "docs/observe/volume-assertions",
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
          label: "Data Products",
          type: "doc",
          id: "docs/dataproducts",
        },
        {
          label: "Incidents",
          type: "doc",
          id: "docs/incidents/incidents",
        },
        {
          label: "Metadata Tests",
          type: "doc",
          id: "docs/tests/metadata-tests",
          className: "saasOnly",
        },
        {
          label: "Sync Status",
          type: "doc",
          id: "docs/sync-status",
        },
      ],
    },
    {
      Notifications: [
        {
          label: "Subscriptions & Notifications",
          type: "doc",
          id: "docs/subscription-and-notification",
          className: "saasOnly",
        },
        {
          Slack: [
            {
              type: "doc",
              id: "docs/slack/saas-slack-setup",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/slack/saas-slack-app",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/slack/saas-slack-troubleshoot",
              className: "saasOnly",
            },
          ],
        },
      ],
    },
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
          id: "docs/lineage/openlineage",
          label: "OpenLineage",
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
        "metadata-ingestion/docs/dev_guides/stateful",
        "metadata-ingestion/docs/dev_guides/classification",
        "metadata-ingestion/docs/dev_guides/add_stateful_ingestion_to_source",
        "metadata-ingestion/docs/dev_guides/sql_profiles",
        "metadata-ingestion/docs/dev_guides/profiling_ingestions",
      ],
    },
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
        "docs/managed-datahub/configuring-identity-provisioning-with-ms-entra",
        "docs/how/backup-datahub",
        "docs/how/restore-indices",
        "docs/advanced/db-retention",
        "docs/advanced/monitoring",
        "docs/deploy/telemetry",
        "docs/how/kafka-config",
        "docs/advanced/no-code-upgrade",
        "docs/how/jattach-guide",
        {
          type: "doc",
          id: "docs/integrations/aws-privatelink",
          label: "AWS PrivateLink Intergration",
          className: "saasOnly",
        },
        {
          type: "doc",
          id: "docs/integrations/oidc-sso-integration",
          className: "saasOnly",
        },
      ],
    },
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
      "Metadata Model": [
        "docs/modeling/metadata-model",
        "docs/modeling/extending-the-metadata-model",
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
        "metadata-ingestion/docs/dev_guides/reporting_telemetry",
        "docs/advanced/mcp-mcl",
        "docker/datahub-upgrade/README",
        "docs/advanced/no-code-modeling",
        "datahub-web-react/src/app/analytics/README",
        "docs/how/migrating-graph-service-implementation",
        "docs/advanced/field-path-spec-v2",
        "metadata-ingestion/adding-source",
        "docs/how/add-custom-ingestion-source",
        "docs/how/add-custom-data-platform",
        "docs/advanced/browse-paths-upgrade",
        "docs/browseV2/browse-paths-v2",
        "docs/plugins",
      ],
    },
    {
      type: "html",
      value: "<div>API & SDKs</div>",
      defaultStyle: true,
    },
    {
      type: "category",
      link: { type: "doc", id: "docs/api/datahub-apis" },
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
      label: "SDK",
      items: [
        {
          "Python SDK": [
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
      ],
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
      Guides: [
        "docs/api/tutorials/custom-properties",
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
        "docs/api/tutorials/ml",
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
    // "docs/how/ui-tabs-guide",
    //"docs/how/build-metadata-service",
    //"docs/how/graph-onboarding",
    //"docs/demo/graph-onboarding",
    //"metadata-integration/java/spark-lineage/README",
    // "metadata-integration/java/spark-lineage-beta/README.md
    // "metadata-integration/java/openlineage-converter/README"
    //"metadata-ingestion-modules/airflow-plugin/README"
    //"metadata-ingestion-modules/dagster-plugin/README"
    // "metadata-ingestion/schedule_docs/datahub", // we can delete this
    // TODO: change the titles of these, removing the "What is..." portion from the sidebar"
    // "docs/what/entity",
    // "docs/what/aspect",
    // "docs/managed-datahub/generated/what/aspect",
    // "docs/managed-datahub/generated/cli",
    // "docs/managed-datahub/generated/authentication/introducing-metadata-service-authentication",
    // "docs/managed-datahub/generated/what/relationship",
    // "docs/managed-datahub/generated/what/entity",
    // "docs/managed-datahub/generated/townhall-history",
    // "docs/managed-datahub/generated/authentication/changing-default-credentials",
    // "docs/managed-datahub/generated/authentication/guides/sso/configure-oidc-react",
    // "docs/managed-datahub/generated/authentication/guides/jaas",
    // "docs/managed-datahub/generated/deploy/aws",
    // "docs/managed-datahub/generated/deploy/kubernetes",
    // "docs/managed-datahub/generated/quickstart",
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
    // ],
  ],
  acrylSidebar: [
    {
      type: "html",
      value: "<div>Overview</div>",
      defaultStyle: true,
    },
    {
      type: "doc",
      id: "docs/managed-datahub/managed-datahub-overview",
      label: "Why DataHub Cloud?",
    },
    {
      type: "doc",
      id: "docs/managed-datahub/welcome-acryl",
      label: "Getting Started With DataHub Cloud",
    },
    {
      type: "html",
      value: "<div>Features</div>",
      defaultStyle: true,
    },
    {
      type: "category",
      label: "Data Discovery",
      items: [
        {
          label: "Business Attributes",
          type: "doc",
          id: "docs/managed-datahub/generated/businessattributes",
        },
        {
          label: "Business Glossary",
          type: "doc",
          id: "docs/managed-datahub/generated/glossary/business-glossary",
        },
        "docs/managed-datahub/generated/features/feature-guides/documentation-forms",
        {
          label: "Domains",
          type: "doc",
          id: "docs/managed-datahub/generated/domains",
        },
        {
          label: "Ingestion",
          type: "doc",
          id: "docs/managed-datahub/generated/ui-ingestion",
        },
        {
          label: "Lineage",
          type: "category",
          link: {
            type: "doc",
            id: "docs/managed-datahub/generated/generated/lineage/lineage-feature-guide",
          },
          items: [
            {
              label: "Lineage Impact analysis",
              type: "doc",
              id: "docs/managed-datahub/generated/act-on-metadata/impact-analysis",
            },
            {
              label: "Managing Lineage via UI",
              type: "doc",
              id: "docs/managed-datahub/generated/features/feature-guides/ui-lineage",
            },
          ],
        },
        {
          label: "Properties",
          type: "doc",
          id: "docs/managed-datahub/generated/features/feature-guides/properties",
        },
        {
          label: "Schema history",
          type: "doc",
          id: "docs/managed-datahub/generated/schema-history",
        },
        {
          label: "Search",
          type: "doc",
          id: "docs/managed-datahub/generated/how/search",
        },
        {
          label: "Tags",
          type: "doc",
          id: "docs/managed-datahub/generated/tags",
        },
        "docs/managed-datahub/generated/features/dataset-usage-and-query-history",
      ],
    },
    {
      type: "category",
      label: "Data Governance",
      items: [
        "docs/managed-datahub/generated/approval-workflows",
        "docs/managed-datahub/generated/chrome-extension",
        {
          label: "Data Contract",
          type: "doc",
          id: "docs/managed-datahub/generated/observe/data-contract",
        },
        {
          label: "Ownership",
          type: "doc",
          id: "docs/managed-datahub/generated/ownership/ownership-types",
        },
        {
          label: "Policies",
          type: "doc",
          id: "docs/managed-datahub/generated/authorization/access-policies-guide",
        },
        "docs/managed-datahub/generated/posts",
      ],
    },
    {
      type: "category",
      label: "Data Quality",
      items: [
        {
          label: "Assertions",
          type: "category",
          link: {
            type: "doc",
            id: "docs/managed-datahub/generated/observe/assertions",
          },
          items: [
            {
              label: "Column Assertions",
              type: "doc",
              id: "docs/managed-datahub/generated/observe/column-assertions",
              className: "saasOnly",
            },
            {
              label: "Custom SQL Assertions",
              type: "doc",
              id: "docs/managed-datahub/generated/observe/custom-sql-assertions",
              className: "saasOnly",
            },
            {
              label: "Freshness Assertions",
              type: "doc",
              id: "docs/managed-datahub/generated/observe/freshness-assertions",
              className: "saasOnly",
            },
            {
              label: "Schema Assertions",
              type: "doc",
              id: "docs/managed-datahub/generated/observe/schema-assertions",
              className: "saasOnly",
            },
            {
              label: "Volume Assertions",
              type: "doc",
              id: "docs/managed-datahub/generated/observe/volume-assertions",
              className: "saasOnly",
            },
            {
              label: "Open Assertions Specification",
              type: "category",
              link: {
                type: "doc",
                id: "docs/managed-datahub/generated/assertions/open-assertions-spec",
              },
              items: [
                {
                  label: "Snowflake",
                  type: "doc",
                  id: "docs/managed-datahub/generated/assertions/snowflake/snowflake_dmfs",
                },
              ],
            },
          ],
        },
        {
          label: "Data Products",
          type: "doc",
          id: "docs/managed-datahub/generated/dataproducts",
        },
        {
          label: "Incidents",
          type: "doc",
          id: "docs/managed-datahub/generated/incidents/incidents",
        },
        {
          label: "Sync Status", // note: to handle errors where you don't want a markdown file in the sidebar, add it as a comment.
          type: "doc",
          id: "docs/managed-datahub/generated/sync-status",
        },
      ],
    },
    {
      Notifications: [
        {
          label: "Subscriptions & Notifications",
          type: "doc",
          id: "docs/managed-datahub/generated/subscription-and-notification",
          className: "saasOnly",
        },
        {
          Slack: [
            {
              type: "doc",
              id: "docs/managed-datahub/generated/slack/saas-slack-setup",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/generated/slack/saas-slack-app",
              className: "saasOnly",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/generated/slack/saas-slack-troubleshoot",
              className: "saasOnly",
            },
          ],
        },
      ],
    },
    {
      type: "html",
      value: "<div>Integrations</div>",
      defaultStyle: true,
    },
    {
      type: "category",
      link: {
        type: "doc",
        id: "docs/managed-datahub/metadata-ingestion/README",
      },
      label: "Overview",
      items: [
        {
          type: "doc",
          label: "Recipe",
          id: "docs/managed-datahub/metadata-ingestion/recipe_overview",
        },
        {
          type: "category",
          label: "Sinks",
          link: {
            type: "doc",
            id: "docs/managed-datahub/metadata-ingestion/sink_overview",
          },
          items: [
            "docs/managed-datahub/metadata-ingestion/sink_docs/console",
            "docs/managed-datahub/metadata-ingestion/sink_docs/datahub",
            "docs/managed-datahub/metadata-ingestion/sink_docs/metadata-file",
          ],
        },
        {
          type: "category",
          label: "Transformers",
          link: {
            type: "doc",
            id: "docs/managed-datahub/metadata-ingestion/docs/transformer/intro",
          },
          items: [
            "docs/managed-datahub/metadata-ingestion/docs/transformer/dataset_transformer",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Sources",
      link: {
        type: "doc",
        id: "docs/managed-datahub/metadata-ingestion/source_overview",
      },
      items: [
        // collapse these; add push-based at top
        {
          type: "doc",
          id: "docs/managed-datahub/generated/lineage/airflow",
          label: "Airflow",
        },
        {
          type: "doc",
          id: "docs/managed-datahub/generated/lineage/dagster",
          label: "Dagster",
        },
        {
          type: "doc",
          id: "docs/managed-datahub/generated/lineage/openlineage",
          label: "OpenLineage",
        },
        {
          type: "doc",
          id: "docs/managed-datahub/metadata-integration/java/acryl-spark-lineage/README",
          label: "Spark",
        },
        //"docker/airflow/local_airflow",
        "docs/managed-datahub/metadata-ingestion/integration_docs/great-expectations",
        "docs/managed-datahub/metadata-integration/java/datahub-protobuf/README",
        //"metadata-integration/java/spark-lineage-legacy/README",
        //"metadata-ingestion/source-docs-template",
        {
          type: "autogenerated",
          dirName: "docs/managed-datahub/generated/generated/ingestion/sources", // '.' means the current docs folder
        },
      ],
    },
    {
      "Advanced Guides": [
        {
          "Scheduling Ingestion": [
            "docs/managed-datahub/metadata-ingestion/schedule_docs/intro",
            "docs/managed-datahub/metadata-ingestion/schedule_docs/cron",
            "docs/managed-datahub/metadata-ingestion/schedule_docs/airflow",
            "docs/managed-datahub/metadata-ingestion/schedule_docs/kubernetes",
          ],
        },
        "docs/managed-datahub/generated/platform-instances",
        "docs/managed-datahub/metadata-ingestion/docs/dev_guides/stateful",
        "docs/managed-datahub/metadata-ingestion/docs/dev_guides/classification",
        "docs/managed-datahub/metadata-ingestion/docs/dev_guides/add_stateful_ingestion_to_source",
        "docs/managed-datahub/metadata-ingestion/docs/dev_guides/sql_profiles",
        "docs/managed-datahub/metadata-ingestion/docs/dev_guides/profiling_ingestions",
      ],
    },
    {
      type: "html",
      value: "<div>Admin</div>",
      defaultStyle: true,
    },
    "docs/managed-datahub/generated/authentication/guides/add-users",
    "docs/managed-datahub/generated/authentication/personal-access-tokens",
    {
      Authorization: [
        "docs/managed-datahub/generated/authorization/README",
        "docs/managed-datahub/generated/authorization/roles",
        "docs/managed-datahub/generated/authorization/policies",
        "docs/managed-datahub/generated/authorization/groups",
      ],
    },
    {
      "Advanced Guides": [
        "docs/managed-datahub/generated/how/delete-metadata",
        {
          type: "doc",
          id: "docs/managed-datahub/generated/integrations/aws-privatelink",
          label: "AWS PrivateLink Intergration",
          className: "saasOnly",
        },
        {
          type: "doc",
          id: "docs/managed-datahub/generated/integrations/oidc-sso-integration",
          className: "saasOnly",
        },
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
      ],
    },
    {
      type: "html",
      value: "<div>Developers</div>",
      defaultStyle: true,
    },
    {
      Architecture: [
        "docs/managed-datahub/generated/architecture/architecture",
        "docs/managed-datahub/generated/components",
        "docs/managed-datahub/generated/architecture/metadata-ingestion",
        "docs/managed-datahub/generated/architecture/metadata-serving",
        "docs/managed-datahub/generated/architecture/docker-containers",
      ],
    },
    {
      "Metadata Model": [
        "docs/managed-datahub/generated/modeling/metadata-model",
        "docs/managed-datahub/generated/modeling/extending-the-metadata-model",
        "docs/managed-datahub/generated/what/mxe",
        {
          Entities: [
            {
              type: "autogenerated",
              dirName:
                "docs/managed-datahub/generated/generated/metamodel/entities", // '.' means the current docs folder
            },
          ],
        },
      ],
    },
    {
      type: "html",
      value: "<div>API & SDKs</div>",
      defaultStyle: true,
    },
    {
      type: "category",
      link: {
        type: "doc",
        id: "docs/managed-datahub/generated/api/datahub-apis",
      },
      label: "API",
      items: [
        {
          "GraphQL API": [
            {
              label: "Overview",
              type: "doc",
              id: "docs/managed-datahub/generated/api/graphql/overview",
            },
            {
              type: "doc",
              id: "docs/managed-datahub/datahub-api/graphql-api/getting-started",
              className: "saasOnly",
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
                  id: "docs/managed-datahub/generated/api/graphql/how-to-set-up-graphql",
                },
                {
                  type: "doc",
                  label: "Getting Started With GraphQL",
                  id: "docs/managed-datahub/generated/api/graphql/getting-started",
                },
                {
                  type: "doc",
                  label: "Access Token Management",
                  id: "docs/managed-datahub/generated/api/graphql/token-management",
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
              id: "docs/managed-datahub/generated/api/openapi/openapi-usage-guide",
            },
          ],
        },
        {
          type: "doc",
          id: "docs/managed-datahub/datahub-api/entity-events-api",
          className: "saasOnly",
        },
        "docs/managed-datahub/generated/dev-guides/timeline",
        {
          "Rest.li API": [
            {
              type: "doc",
              label: "Rest.li API Guide",
              id: "docs/managed-datahub/generated/api/restli/restli-overview",
            },
            {
              type: "doc",
              label: "Restore Indices",
              id: "docs/managed-datahub/generated/api/restli/restore-indices",
            },
            {
              type: "doc",
              label: "Get Index Sizes",
              id: "docs/managed-datahub/generated/api/restli/get-index-sizes",
            },
            {
              type: "doc",
              label: "Truncate Timeseries Aspect",
              id: "docs/managed-datahub/generated/api/restli/truncate-time-series-aspect",
            },
            {
              type: "doc",
              label: "Get ElasticSearch Task Status Endpoint",
              id: "docs/managed-datahub/generated/api/restli/get-elastic-task-status",
            },
            {
              type: "doc",
              label: "Evaluate Tests",
              id: "docs/managed-datahub/generated/api/restli/evaluate-tests",
            },
            {
              type: "doc",
              label: "Aspect Versioning and Rest.li Modeling",
              id: "docs/managed-datahub/generated/advanced/aspect-versioning",
            },
          ],
        },
      ],
    },
    {
      type: "category",
      label: "SDK",
      items: [
        {
          "Python SDK": [
            "docs/managed-datahub/metadata-ingestion/as-a-library",
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
          id: "docs/managed-datahub/metadata-integration/java/as-a-library",
        },
      ],
    },
    {
      type: "category",
      label: "DataHub CLI",
      link: { type: "doc", id: "docs/cli" },
      items: ["docs/managed-datahub/generated/datahub_lite"],
    },
    {
      type: "category",
      label: "Datahub Actions",
      link: {
        type: "doc",
        id: "docs/managed-datahub/generated/act-on-metadata",
      },
      items: [
        "docs/managed-datahub/generated/actions/README",
        "docs/managed-datahub/generated/actions/quickstart",
        "docs/managed-datahub/generated/actions/concepts",
        {
          Sources: [
            "docs/managed-datahub/generated/actions/sources/kafka-event-source",
          ],
        },
        {
          Events: [
            "docs/managed-datahub/generated/actions/events/entity-change-event",
            "docs/managed-datahub/generated/actions/events/metadata-change-log-event",
          ],
        },
        {
          Actions: [
            "docs/managed-datahub/generated/actions/actions/executor",
            "docs/managed-datahub/generated/actions/actions/hello_world",
            "docs/managed-datahub/generated/actions/actions/slack",
            "docs/managed-datahub/generated/actions/actions/teams",
          ],
        },
        {
          Guides: [
            "docs/managed-datahub/generated/actions/guides/developing-a-transformer",
            "docs/managed-datahub/generated/actions/guides/developing-an-action",
          ],
        },
      ],
    },
    {
      Guides: [
        "docs/managed-datahub/generated/api/tutorials/custom-properties",
        "docs/managed-datahub/generated/api/tutorials/datasets",
        "docs/managed-datahub/generated/api/tutorials/deprecation",
        "docs/managed-datahub/generated/api/tutorials/descriptions",
        "docs/managed-datahub/generated/api/tutorials/custom-properties",
        "docs/managed-datahub/generated/api/tutorials/assertions",
        "docs/managed-datahub/generated/api/tutorials/custom-assertions",
        "docs/managed-datahub/generated/api/tutorials/incidents",
        "docs/managed-datahub/generated/api/tutorials/operations",
        "docs/managed-datahub/generated/api/tutorials/data-contracts",
        "docs/managed-datahub/generated/api/tutorials/domains",
        "docs/managed-datahub/generated/api/tutorials/forms",
        "docs/managed-datahub/generated/api/tutorials/lineage",
        "docs/managed-datahub/generated/api/tutorials/ml",
        "docs/managed-datahub/generated/api/tutorials/owners",
        "docs/managed-datahub/generated/api/tutorials/structured-properties",
        "docs/managed-datahub/generated/api/tutorials/tags",
        "docs/managed-datahub/generated/api/tutorials/terms",
        {
          type: "doc",
          id: "docs/managed-datahub/generated/advanced/patch",
          label: "Patch",
        },
      ],
    },
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
        "docs/managed-datahub/generated/slack",
        "docs/managed-datahub/generated/townhalls",
        //        "docs/townhall-history",
        "docs/managed-datahub/generated/CODE_OF_CONDUCT",
        "docs/managed-datahub/generated/CONTRIBUTING",
        "docs/managed-datahub/generated/links",
        "docs/managed-datahub/generated/rfc",
      ],
    },
    {
      "Acryl Cloud Release History": [
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
};
