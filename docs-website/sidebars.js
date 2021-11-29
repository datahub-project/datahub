const fs = require("fs");

function list_ids_in_directory(directory, hardcoded_labels) {
  if (hardcoded_labels === undefined) {
    hardcoded_labels = {};
  }

  const files = fs.readdirSync(`../${directory}`).sort();
  let ids = [];
  for (const name of files) {
    if (fs.lstatSync(`../${directory}/${name}`).isDirectory()) {
      // Recurse into the directory.
      const inner_ids = list_ids_in_directory(`${directory}/${name}`);
      ids = ids.concat(inner_ids);
    } else {
      if (name.endsWith(".md")) {
        const slug = name.replace(/\.md$/, "");
        let id = `${directory}/${slug}`;
        if (id.match(/\/\d+-.+/)) {
          id = id.replace(/\/\d+-/, "/");
        }

        if (id in hardcoded_labels) {
          label = hardcoded_labels[id];
          ids.push({ type: "doc", id, label });
        } else {
          ids.push({ type: "doc", id });
        }
      }
    }
  }
  return ids;
}

// note: to handle errors where you don't want a markdown file in the sidebar, add it as a comment.
// this will fix errors like `Error: File not accounted for in sidebar: ...`
module.exports = {
  // users
  // architects
  // modelers
  // developers
  // operators

  overviewSidebar: {
    DataHub: [
      "README",
      // "docs/faq", // hide from sidebar: out of date
      "docs/features",
      {
        Architecture: [
          "docs/architecture/architecture",
          "docs/components",
          "docs/architecture/metadata-ingestion",
          "docs/architecture/metadata-serving",
          // "docs/what/gma",
          // "docs/what/gms",
        ],
      },
      "docs/roadmap",
      "docs/CONTRIBUTING",
      "docs/demo",
      "docs/saas",
      "releases",
    ],
    "Getting Started": [
      "docs/quickstart",
      "docs/cli",
      "metadata-ingestion/README",
      "docs/debugging",
    ],
    "Metadata Ingestion": [
      // add a custom label since the default is 'Metadata Ingestion'
      // note that we also have to add the path to this file in sidebarsjs_hardcoded_titles in generateDocsDir.ts
      {
        type: "doc",
        label: "Quickstart",
        id: "metadata-ingestion/README",
      },
      {
        Sources: list_ids_in_directory("metadata-ingestion/source_docs", {
          "metadata-ingestion/source_docs/s3": "S3",
        }),
      },
      "metadata-ingestion/transformers",
      {
        Sinks: list_ids_in_directory("metadata-ingestion/sink_docs"),
      },
    ],
    "Metadata Modeling": [
      "docs/modeling/metadata-model",
      "docs/modeling/extending-the-metadata-model",
      // TODO: change the titles of these, removing the "What is..." portion from the sidebar"
      // "docs/what/entity",
      // "docs/what/aspect",
      // "docs/what/urn",
      // "docs/what/relationship",
      // "docs/what/search-document",
      // "docs/what/snapshot",
      // "docs/what/delta",
      // "docs/what/mxe",
    ],
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
            label: "Getting Started",
            id: "docs/api/graphql/getting-started",
          },
          {
            type: "doc",
            label: "Querying Metadata Entities",
            id: "docs/api/graphql/querying-entities",
          },
        ],
      },
    ],
    "Usage Guides": ["docs/policies"],
    "Developer Guides": [
      // TODO: the titles of these should not be in question form in the sidebar
      "docs/developers",
      "docs/docker/development",
      "metadata-ingestion/adding-source",
      {
        type: "doc",
        label: "Ingesting files from S3",
        id: "metadata-ingestion/source_docs/s3",
      },
      //"metadata-ingestion/examples/transforms/README"
      //"docs/what/graph",
      //"docs/what/search-index",
      //"docs/how/add-new-aspect",
      //"docs/how/build-metadata-service",
      //"docs/how/graph-onboarding",
      //"docs/demo/graph-onboarding",
      "docs/how/auth/jaas",
      "docs/how/auth/sso/configure-oidc-react",
      "docs/how/auth/sso/configure-oidc-react-google",
      "docs/how/auth/sso/configure-oidc-react-okta",
      "docs/how/restore-indices",
      "docs/how/extract-container-logs",
      "docs/how/delete-metadata",
      "datahub-web-react/src/app/analytics/README",
      "metadata-ingestion/developing",
      "docker/airflow/local_airflow",
      "docs/how/add-custom-data-platform",
      "docs/how/add-custom-ingestion-source",
      {
        "Module READMEs": [
          "datahub-web-react/README",
          "datahub-frontend/README",
          "datahub-graphql-core/README",
          "metadata-service/README",
          // "metadata-jobs/README",
          "metadata-jobs/mae-consumer-job/README",
          "metadata-jobs/mce-consumer-job/README",
        ],
      },
      {
        Advanced: [
          "docs/advanced/no-code-modeling",
          "docs/advanced/aspect-versioning",
          "docs/advanced/es-7-upgrade",
          "docs/advanced/high-cardinality",
          "docs/advanced/no-code-upgrade",
          "docs/how/migrating-graph-service-implementation",
          "docs/advanced/mcp-mcl",
          "docs/advanced/field-path-spec-v2",
          "docs/advanced/monitoring",
          // WIP "docs/advanced/backfilling",
          // WIP "docs/advanced/derived-aspects",
          // WIP "docs/advanced/entity-hierarchy",
          // WIP "docs/advanced/partial-update",
          // WIP "docs/advanced/pdl-best-practices",
        ],
      },
    ],
    "Deployment Guides": [
      "docs/how/kafka-config",
      "docker/README",
      "docs/deploy/kubernetes",
      "docker/datahub-upgrade/README",
      "docs/deploy/aws",
      "docs/deploy/gcp",
      "docs/deploy/confluent-cloud",
      // Purposely not including the following:
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
    ],
    Community: [
      "docs/slack",
      "docs/links",
      "docs/townhalls",
      "docs/townhall-history",
      "docs/CODE_OF_CONDUCT",
      "docs/rfc",
      {
        RFCs: list_ids_in_directory("docs/rfc/active"),
      },
    ],
  },
};
