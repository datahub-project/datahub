module.exports = {
  someSidebar: {
    'DataHub': [
      'README',
      'docs/faq',
      'docs/features',
      'docs/roadmap',
      'docs/CONTRIBUTING',
    ],
    'Getting Started': [
      'docs/quickstart',
      'docs/debugging',
    ],
    'Architecture': [
      'docs/README',
      'docs/architecture/architecture',
      'docs/architecture/metadata-ingestion',
      'docs/architecture/metadata-serving',
    ],
    'Concepts': [
      "docs/what/aspect",
      "docs/what/delta",
      "docs/what/entity",
      "docs/what/gma",
      "docs/what/gms",
      "docs/what/graph",
      "docs/what/mxe",
      "docs/what/relationship",
      "docs/what/search-document",
      "docs/what/search-index",
      "docs/what/snapshot",
      "docs/what/urn",
    ],
    'Guides': [
      "docs/how/add-new-aspect",
      "docs/how/customize-elasticsearch-query-template",
      "docs/how/data-source-onboarding",
      "docs/how/entity-onboarding",
      "docs/how/graph-onboarding",
      "docs/how/kafka-config",
      "docs/how/metadata-modelling",
      "docs/demo/graph-onboarding",
      "docs/how/scsi-onboarding-guide",
      "docs/how/search-onboarding",
      "docs/how/search-over-new-field",
    ],
    'Advanced Guides': [
      "docs/advanced/aspect-versioning",
      "docs/advanced/backfilling",
      "docs/advanced/derived-aspects",
      "docs/advanced/entity-hierarchy",
      "docs/advanced/high-cardinality",
      "docs/advanced/partial-update",
      "docs/advanced/pdl-best-practices",
    ],
    'Developing': [
      'docs/developers',
      'docs/docker/development',
      {
        'Components': [
          'datahub-web-react/README',
          'datahub-graphql-core/README',
          'datahub-frontend/README',
          'datahub-gms-graphql-service/README',
          'gms/README',
          'metadata-jobs/README',
          'metadata-jobs/mae-consumer-job/README',
          'metadata-jobs/mce-consumer-job/README',
          'metadata-ingestion/README',
        ]
      },
      'docs/rfc',
      {
        "RFCs": [
          "docs/rfc/active/1778-dashboards/README",
          "docs/rfc/active/1812-ml_models/README",
          "docs/rfc/active/1820-azkaban-flow-job/README",
          "docs/rfc/active/1841-lineage/field level lineage",
          "docs/rfc/active/business_glossary/README",
          "docs/rfc/active/graph_ql_frontend/queries",
          "docs/rfc/active/react-app/README",
        ]
      },
    ],
    'Community': [
      'docs/slack',
      'docs/links',
      'docs/townhalls',
      'docs/townhall-history',
      'docs/CODE_OF_CONDUCT',
    ]
  },
};
