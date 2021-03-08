module.exports = {
  // users
  // architects
  // modelers
  // developers
  // operators

  overviewSidebar: {
    DataHub: [
      "README",
      "docs/faq",
      "docs/features",
      "docs/roadmap",
      "docs/CONTRIBUTING",
      "docs/demo",
    ],
    "Getting Started": [
      // Serves as user guides.
      "docs/quickstart",
      "docs/debugging",
      // TODO "docs/how/data-source-onboarding",
    ],
    Architecture: [
      // "docs/README",
      "docs/architecture/architecture",
      "docs/architecture/metadata-ingestion",
      "docs/what/gma",
      "docs/architecture/metadata-serving",
      "docs/what/gms",
    ],
    // },
    // developerGuideSidebar: {
    "Metadata Modeling": [
      // TODO: change the titles of these, removing the "What is..." portion from the sidebar"
      "docs/what/entity",
      "docs/what/aspect",
      "docs/what/urn",
      "docs/what/relationship",
      "docs/what/search-document",
      "docs/what/snapshot",
      "docs/what/delta",
      "docs/what/mxe",
    ],
    "Developer Guides": [
      // TODO: the titles of these should not be in question form in the sidebar
      "docs/developers",
      "docs/docker/development",
      "docs/what/graph",
      "docs/what/search-index",
      "docs/how/add-new-aspect",
      "docs/how/customize-elasticsearch-query-template",
      "docs/how/entity-onboarding",
      "docs/how/graph-onboarding",
      "docs/how/metadata-modelling",
      "docs/demo/graph-onboarding",
      "docs/how/search-onboarding",
      "docs/how/search-over-new-field",
    ],
    Components: [
      "datahub-web-react/README",
      "datahub-frontend/README",
      "datahub-graphql-core/README",
      "gms/README",
      "datahub-gms-graphql-service/README",
      // "metadata-jobs/README",
      "metadata-jobs/mae-consumer-job/README",
      "metadata-jobs/mce-consumer-job/README",
      "metadata-ingestion/README",
    ],
    "Advanced Guides": [
      "docs/advanced/aspect-versioning",
      "docs/advanced/high-cardinality",
      "docs/how/scsi-onboarding-guide",
      // WIP "docs/advanced/backfilling",
      // WIP "docs/advanced/derived-aspects",
      // WIP "docs/advanced/entity-hierarchy",
      // WIP "docs/advanced/partial-update",
      // WIP "docs/advanced/pdl-best-practices",
    ],
    // },
    // operatorGuideSidebar: {
    Deployment: [
      "docs/how/kafka-config",
      "docker/README",
      "contrib/kubernetes/README",
      // Purposely not including the following:
      // - "docker/datahub-frontend/README",
      // - "docker/datahub-gms-graphql-service/README",
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
    ],
    // },
    Community: [
      "docs/slack",
      "docs/links",
      "docs/townhalls",
      "docs/townhall-history",
      "docs/CODE_OF_CONDUCT",
      "docs/rfc",
      {
        RFCs: [
          "docs/rfc/active/1778-dashboards/README",
          "docs/rfc/active/1812-ml_models/README",
          "docs/rfc/active/1820-azkaban-flow-job/README",
          "docs/rfc/active/1841-lineage/field_level_lineage",
          "docs/rfc/active/business_glossary/README",
          "docs/rfc/active/graph_ql_frontend/queries",
          "docs/rfc/active/react-app/README",
          "docs/rfc/active/tags/README",
        ],
      },
    ],
  },
};
