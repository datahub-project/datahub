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
# Anything not matched here is ignored (its `label` still lands as a custom
# property via the fallback, but without a value we skip it).
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
          ... on LocalFileCodeReference { filePath lineNumber }
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

# Single round-trip: code locations -> repositories -> jobs + asset nodes.
# Scoping assets under their repository gives us the code-location name used to
# build DataFlow/DataJob ids that match the existing push plugin's convention.
REPOSITORIES_QUERY = (
    """
query DatahubDagsterIngest {
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
        assetNodes {
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
