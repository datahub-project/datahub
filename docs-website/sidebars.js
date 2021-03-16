const fs = require("fs");

function list_ids_in_directory(directory) {
  const files = fs.readdirSync(`../${directory}`);
  let ids = [];
  for (const name of files) {
    if (fs.lstatSync(`../${directory}/${name}`).isDirectory()) {
      // Recurse into the directory.
      const inner_ids = list_ids_in_directory(`${directory}/${name}`);
      ids = ids.concat(inner_ids);
    } else {
      if (name.endsWith(".md")) {
        const id = `${directory}/${name}`.replace(/\.md$/, "");
        ids.push(id);
      }
    }
  }
  ids.sort();
  return ids;
}

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
      "docs/architecture/architecture",
      "docs/architecture/metadata-ingestion",
      //"docs/what/gma",
      "docs/architecture/metadata-serving",
      //"docs/what/gms",
      "datahub-web-react/README",
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
      "docs/how/build-metadata-service",
      "docs/how/customize-elasticsearch-query-template",
      "docs/how/entity-onboarding",
      "docs/how/graph-onboarding",
      "docs/how/metadata-modelling",
      "docs/demo/graph-onboarding",
      "docs/how/search-onboarding",
      "docs/how/search-over-new-field",
      "docs/how/configure-oidc-react",
      "docs/how/sso/configure-oidc-react-google",
      "docs/how/sso/configure-oidc-react-okta",
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
        RFCs: list_ids_in_directory("docs/rfc/active"),
      },
    ],
  },
};
