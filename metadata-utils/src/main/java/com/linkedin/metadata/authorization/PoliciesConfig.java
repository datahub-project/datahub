package com.linkedin.metadata.authorization;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;


/**
 * This policies config file defines the base set of privileges that DataHub supports.
 */
public class PoliciesConfig {

  public static final String PLATFORM_POLICY_TYPE = "PLATFORM";
  public static final String METADATA_POLICY_TYPE = "METADATA";
  public static final String ACTIVE_POLICY_STATE = "ACTIVE";
  public static final String INACTIVE_POLICY_STATE = "INACTIVE";

  // Platform Privileges //

  public static final Privilege MANAGE_POLICIES_PRIVILEGE = Privilege.of(
      "MANAGE_POLICIES",
      "Manage Policies",
      "Create and remove access control policies. Be careful - Actors with this privilege are effectively super users.");

  public static final Privilege MANAGE_USERS_AND_GROUPS_PRIVILEGE = Privilege.of(
      "MANAGE_USERS_AND_GROUPS",
      "Manage Users & Groups",
      "Create, remove, and update users and groups on DataHub.");

  public static final Privilege VIEW_ANALYTICS_PRIVILEGE = Privilege.of(
      "VIEW_ANALYTICS",
      "View Analytics",
      "View the DataHub analytics dashboard.");

  public static final Privilege GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE = Privilege.of(
      "GENERATE_PERSONAL_ACCESS_TOKENS",
      "Generate Personal Access Tokens",
      "Generate personal access tokens for use with DataHub APIs.");

  public static final List<Privilege> PLATFORM_PRIVILEGES = ImmutableList.of(
      MANAGE_POLICIES_PRIVILEGE,
      MANAGE_USERS_AND_GROUPS_PRIVILEGE,
      VIEW_ANALYTICS_PRIVILEGE,
      GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE
  );

  // Resource Privileges //

  public static final Privilege EDIT_ENTITY_TAGS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_TAGS",
      "Edit Tags",
      "The ability to add and remove tags to an asset.");

  public static final Privilege EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_GLOSSARY_TERMS",
      "Edit Glossary Terms",
      "The ability to add and remove glossary terms to an asset.");

  public static final Privilege EDIT_ENTITY_OWNERS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_OWNERS",
      "Edit Owners",
      "The ability to add and remove owners of an asset.");

  public static final Privilege EDIT_ENTITY_DOCS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_DOCS",
      "Edit Documentation",
      "The ability to edit documentation about an asset.");

  public static final Privilege EDIT_ENTITY_DOC_LINKS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_DOC_LINKS",
      "Edit Links",
      "The ability to edit links associated with an asset.");

  public static final Privilege EDIT_ENTITY_STATUS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_STATUS",
      "Edit Status",
      "The ability to edit the status of an entity (soft deleted or not).");

  public static final Privilege EDIT_ENTITY_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY",
      "Edit All",
      "The ability to edit any information about an asset. Super user privileges.");

  public static final List<Privilege> COMMON_ENTITY_PRIVILEGES = ImmutableList.of(
      EDIT_ENTITY_TAGS_PRIVILEGE,
      EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE,
      EDIT_ENTITY_OWNERS_PRIVILEGE,
      EDIT_ENTITY_DOCS_PRIVILEGE,
      EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
      EDIT_ENTITY_STATUS_PRIVILEGE,
      EDIT_ENTITY_PRIVILEGE
  );

  // Dataset Privileges
  public static final Privilege EDIT_DATASET_COL_TAGS_PRIVILEGE = Privilege.of(
      "EDIT_DATASET_COL_TAGS",
      "Edit Dataset Column Tags",
      "The ability to edit the column (field) tags associated with a dataset schema."
  );

  public static final Privilege EDIT_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE = Privilege.of(
      "EDIT_DATASET_COL_GLOSSARY_TERMS",
      "Edit Dataset Column Glossary Terms",
      "The ability to edit the column (field) glossary terms associated with a dataset schema."
  );

  public static final Privilege EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE = Privilege.of(
      "EDIT_DATASET_COL_DESCRIPTION",
      "Edit Dataset Column Descriptions",
      "The ability to edit the column (field) descriptions associated with a dataset schema."
  );

  public static final ResourcePrivileges DATASET_PRIVILEGES = ResourcePrivileges.of(
      "dataset",
      "Datasets",
      "Datasets indexed by DataHub", Stream.of(
          COMMON_ENTITY_PRIVILEGES,
          ImmutableList.of(EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE, EDIT_DATASET_COL_TAGS_PRIVILEGE, EDIT_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE))
          .flatMap(Collection::stream)
          .collect(Collectors.toList())
  );

  // Charts Privileges
  public static final ResourcePrivileges CHART_PRIVILEGES = ResourcePrivileges.of(
      "chart",
      "Charts",
      "Charts indexed by DataHub",
      COMMON_ENTITY_PRIVILEGES
  );

  // Dashboard Privileges
  public static final ResourcePrivileges DASHBOARD_PRIVILEGES = ResourcePrivileges.of(
      "dashboard",
      "Dashboards",
      "Dashboards indexed by DataHub",
      COMMON_ENTITY_PRIVILEGES
  );

  // Data Flow Privileges
  public static final ResourcePrivileges DATA_FLOW_PRIVILEGES = ResourcePrivileges.of(
      "dataFlow",
      "Data Pipelines",
      "Data Pipelines indexed by DataHub",
      COMMON_ENTITY_PRIVILEGES
  );

  // Data Job Privileges
  public static final ResourcePrivileges DATA_JOB_PRIVILEGES = ResourcePrivileges.of(
      "dataJob",
      "Data Tasks",
      "Data Tasks indexed by DataHub",
      COMMON_ENTITY_PRIVILEGES
  );

  // Tag Privileges
  public static final ResourcePrivileges TAG_PRIVILEGES = ResourcePrivileges.of(
      "tag",
      "Tags",
      "Tags indexed by DataHub",
      ImmutableList.of(EDIT_ENTITY_OWNERS_PRIVILEGE, EDIT_ENTITY_PRIVILEGE)
  );

  public static final List<ResourcePrivileges> RESOURCE_PRIVILEGES = ImmutableList.of(
      DATASET_PRIVILEGES,
      DASHBOARD_PRIVILEGES,
      CHART_PRIVILEGES,
      DATA_FLOW_PRIVILEGES,
      DATA_JOB_PRIVILEGES,
      TAG_PRIVILEGES
  );

  @Data
  @Getter
  @AllArgsConstructor
  public static class Privilege {
    private String type;
    private String displayName;
    private String description;

    static Privilege of(String type, String displayName, String description) {
      return new Privilege(type, displayName, description);
    }
  }

  @Data
  @Getter
  @AllArgsConstructor
  public static class ResourcePrivileges {
    private String resourceType;
    private String resourceTypeDisplayName;
    private String resourceTypeDescription;
    private List<Privilege> privileges;

    static ResourcePrivileges of(
        String resourceType,
        String resourceTypeDisplayName,
        String resourceTypeDescription,
        List<Privilege> privileges) {
      return new ResourcePrivileges(resourceType, resourceTypeDisplayName, resourceTypeDescription, privileges);
    }
  }

  private PoliciesConfig() { }
}
