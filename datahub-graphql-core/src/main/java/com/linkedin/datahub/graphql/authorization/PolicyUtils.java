package com.linkedin.datahub.graphql.authorization;

/**
 * Notice that this **must** be the same as those resource types presented via the UI.
 *
 * TODO: Pull resource types from the UI.
 * TODO: Move this into a common utility module so that GMS could presumably use it.
 */
public class PolicyUtils {

  public static final String DATASET_ENTITY_RESOURCE_TYPE = "DATASET";

  // Common Privileges
  public static final String EDIT_ENTITY_TAGS = "EDIT_ENTITY_TAGS";
  public static final String EDIT_ENTITY_OWNERS = "EDIT_ENTITY_OWNERS";
  public static final String EDIT_ENTITY_DOCS = "EDIT_ENTITY_DOCS";
  public static final String EDIT_ENTITY_DOC_LINKS = "EDIT_ENTITY_DOC_LINKS";
  public static final String EDIT_ENTITY_STATUS = "EDIT_ENTITY_STATUS";
  public static final String EDIT_ENTITY = "EDIT_ENTITY";

  // Dataset-specific privileges
  public static final String EDIT_DATASET_COL_TAGS = "EDIT_DATASET_COL_TAGS";
  public static final String EDIT_DATASET_COL_DESCRIPTION = "EDIT_DATASET_COL_DESCRIPTION";

  private PolicyUtils() { }
}
