"""GraphQL queries for the pull-based Dagster source.

These query strings are issued as raw GraphQL against the Dagster webserver's
``/graphql`` endpoint. They work unchanged against Dagster OSS and Dagster+; only
the URL and auth header differ (see ``dagster_api.py``). Field names were verified
against the authoritative schema (``js_modules/ui-core/src/graphql/schema.graphql``
in dagster-io/dagster).
"""

# Owner union fragment — assets expose AssetOwner, jobs expose DefinitionOwner.
# Both resolve to a user email or a team name.
_ASSET_OWNER_FRAGMENT = """
    owners {
      __typename
      ... on UserAssetOwner { email }
      ... on TeamAssetOwner { team }
    }
"""

_DEFINITION_OWNER_FRAGMENT = """
    owners {
      __typename
      ... on UserDefinitionOwner { email }
      ... on TeamDefinitionOwner { team }
    }
"""

# Doc/link/schema-bearing MetadataEntry implementations plus scalar entries.
# Note: LocalFileCodeReference entries are intentionally not surfaced as links —
# local file paths are not resolvable URLs in DataHub — so only the `url` of a
# UrlCodeReference is requested below.
_METADATA_ENTRIES_FRAGMENT = """
    metadataEntries {
      __typename
      label
      ... on TextMetadataEntry { text }
      ... on UrlMetadataEntry { url }
      ... on MarkdownMetadataEntry { mdStr }
      ... on PathMetadataEntry { path }
      ... on JsonMetadataEntry { jsonString }
      ... on BoolMetadataEntry { boolValue }
      ... on IntMetadataEntry { intRepr }
      ... on FloatMetadataEntry { floatRepr }
      ... on CodeReferencesMetadataEntry {
        codeReferences {
          __typename
          ... on UrlCodeReference { url }
        }
      }
      ... on TableSchemaMetadataEntry {
        schema {
          columns {
            name
            type
            description
            constraints { nullable }
          }
        }
      }
      ... on TableColumnLineageMetadataEntry {
        lineage {
          columnName
          columnDeps {
            assetKey { path }
            columnName
          }
        }
      }
    }
"""

_ASSET_NODE_FRAGMENT = (
    """
    assetKey { path }
    groupName
    opNames
    jobNames
    description
    computeKind
    kinds
    dependencyKeys { path }
    dependedByKeys { path }
"""
    + _ASSET_OWNER_FRAGMENT
    + """
    tags { key value }
"""
    + _METADATA_ENTRIES_FRAGMENT
)

# Lightweight first pass: code locations -> repositories -> jobs. Assets are
# fetched separately (and paginated) per repository via ASSET_NODES_QUERY, so a
# repository with tens of thousands of assets does not have to come back in a
# single response.
REPOSITORIES_QUERY = (
    """
query DatahubDagsterRepositories {
  repositoriesOrError {
    __typename
    ... on RepositoryConnection {
      nodes {
        name
        location { name }
        pipelines {
          name
          description
          tags { key value }
"""
    + _DEFINITION_OWNER_FRAGMENT
    + """
        }
      }
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""
)

# Cursor-paginated asset fetch for a single repository. `assetNodesConnection`
# returns `hasMore` + a `cursor` to drive the loop in the client.
ASSET_NODES_QUERY = (
    """
query DatahubDagsterAssets(
  $selector: RepositorySelector!
  $cursor: String
  $limit: Int!
) {
  repositoryOrError(repositorySelector: $selector) {
    __typename
    ... on Repository {
      assetNodesConnection(cursor: $cursor, limit: $limit) {
        cursor
        hasMore
        nodes {
"""
    + _ASSET_NODE_FRAGMENT
    + """
        }
      }
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""
)
