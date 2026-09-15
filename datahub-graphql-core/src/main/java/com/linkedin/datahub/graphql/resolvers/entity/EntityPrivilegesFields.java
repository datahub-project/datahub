package com.linkedin.datahub.graphql.resolvers.entity;

/**
 * String constants for the fields of the generated {@link
 * com.linkedin.datahub.graphql.generated.EntityPrivileges} GraphQL type.
 *
 * <p>The codegen produces an {@code EntityPrivileges} POJO but does not expose its field names as
 * strings, yet {@link EntityPrivilegesResolver} needs them to honor the GraphQL selection set.
 * Centralizing them here avoids repeating hardcoded literals and gives a single source of truth.
 *
 * <p>{@code EntityPrivilegesFieldsTest} reflectively cross-checks these constants against the
 * generated type in both directions, so a typo here — or drift if the GraphQL definition changes —
 * fails the build rather than silently leaving a privilege uncomputed.
 */
public final class EntityPrivilegesFields {

  private EntityPrivilegesFields() {}

  public static final String CAN_MANAGE_CHILDREN = "canManageChildren";
  public static final String CAN_MANAGE_ENTITY = "canManageEntity";
  public static final String CAN_EDIT_LINEAGE = "canEditLineage";
  public static final String CAN_EDIT_EMBED = "canEditEmbed";
  public static final String CAN_EDIT_QUERIES = "canEditQueries";
  public static final String CAN_EDIT_PROPERTIES = "canEditProperties";
  public static final String CAN_EDIT_TAGS = "canEditTags";
  public static final String CAN_EDIT_GLOSSARY_TERMS = "canEditGlossaryTerms";
  public static final String CAN_EDIT_DESCRIPTION = "canEditDescription";
  public static final String CAN_EDIT_LINKS = "canEditLinks";
  public static final String CAN_EDIT_DOMAINS = "canEditDomains";
  public static final String CAN_EDIT_DATA_PRODUCTS = "canEditDataProducts";
  public static final String CAN_EDIT_OWNERS = "canEditOwners";
  public static final String CAN_EDIT_INCIDENTS = "canEditIncidents";
  public static final String CAN_EDIT_ASSERTIONS = "canEditAssertions";
  public static final String CAN_EDIT_ASSERTION_OWNERS = "canEditAssertionOwners";
  public static final String CAN_EDIT_DEPRECATION = "canEditDeprecation";
  public static final String CAN_EDIT_SCHEMA_FIELD_TAGS = "canEditSchemaFieldTags";
  public static final String CAN_EDIT_SCHEMA_FIELD_GLOSSARY_TERMS =
      "canEditSchemaFieldGlossaryTerms";
  public static final String CAN_EDIT_SCHEMA_FIELD_DESCRIPTION = "canEditSchemaFieldDescription";
  public static final String CAN_VIEW_DATASET_USAGE = "canViewDatasetUsage";
  public static final String CAN_VIEW_DATASET_PROFILE = "canViewDatasetProfile";
  public static final String CAN_VIEW_DATASET_OPERATIONS = "canViewDatasetOperations";
  public static final String CAN_MANAGE_ASSET_SUMMARY = "canManageAssetSummary";
}
