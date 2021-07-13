const fs = require("fs");

function list_ids_in_directory(directory) {
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
        ids.push(id);
      }
    }
  }
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
      // "docs/faq", // hide from sidebar: out of date
      "docs/features",
      "docs/roadmap",
      "docs/CONTRIBUTING",
      "docs/demo",
      "docs/saas",
      "releases",
    ],
    "Getting Started": [
      // Serves as user guides.
      "docs/quickstart",
      "docs/debugging",
      "metadata-ingestion/README",
    ],
    Architecture: [
      "docs/architecture/architecture",
      "docs/architecture/metadata-ingestion",
      //"docs/what/gma",
      "docs/architecture/metadata-serving",
      //"docs/what/gms",
      "datahub-web-react/README",
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
    "Developer Guides": [
      // TODO: the titles of these should not be in question form in the sidebar
      "docs/developers",
      "docs/docker/development",
      "metadata-ingestion/adding-source",
      "metadata-ingestion/s3-ingestion",
      //"docs/what/graph",
      //"docs/what/search-index",
      //"docs/how/add-new-aspect",
      //"docs/how/build-metadata-service",
      //"docs/how/customize-elasticsearch-query-template",
      //"docs/how/entity-onboarding",
      //"docs/how/graph-onboarding",
      // "docs/how/metadata-modelling",
      //"docs/demo/graph-onboarding",
      //"docs/how/search-onboarding",
      //"docs/how/search-over-new-field",
      "docs/how/auth/jaas",
      "docs/how/auth/sso/configure-oidc-react",
      "docs/how/auth/sso/configure-oidc-react-google",
      "docs/how/auth/sso/configure-oidc-react-okta",
      "docs/how/restore-indices",
      "datahub-web-react/src/app/analytics/README",
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
      "metadata-ingestion/developing",
    ],
    "Advanced Guides": [
      "docs/advanced/no-code-modeling",
      "docs/advanced/aspect-versioning",
      "docs/advanced/es-7-upgrade",
      "docs/advanced/high-cardinality",
      "docs/how/scsi-onboarding-guide",
      "docs/advanced/no-code-upgrade",
      "docs/how/migrating-graph-service-implementation",
      // WIP "docs/advanced/backfilling",
      // WIP "docs/advanced/derived-aspects",
      // WIP "docs/advanced/entity-hierarchy",
      // WIP "docs/advanced/partial-update",
      // WIP "docs/advanced/pdl-best-practices",
    ],
    Deployment: [
      "docs/how/kafka-config",
      "docker/README",
      "docs/deploy/kubernetes",
      "docker/datahub-upgrade/README",
      "docs/deploy/aws",
      "docs/deploy/gcp",
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
