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

  public static final Privilege MANAGE_INGESTION_PRIVILEGE = Privilege.of(
      "MANAGE_INGESTION",
      "Manage Metadata Ingestion",
      "Create, remove, and update Metadata Ingestion sources.");

  public static final Privilege MANAGE_SECRETS_PRIVILEGE = Privilege.of(
      "MANAGE_SECRETS",
      "Manage Secrets",
      "Create & remove Secrets stored inside DataHub.");

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

  public static final Privilege MANAGE_ACCESS_TOKENS = Privilege.of(
      "MANAGE_ACCESS_TOKENS",
      "Manage All Access Tokens",
      "Create, list and revoke access tokens on behalf of users in DataHub. Be careful - Actors with this "
          + "privilege are effectively super users that can impersonate other users."
  );

  public static final Privilege MANAGE_DOMAINS_PRIVILEGE = Privilege.of(
      "MANAGE_DOMAINS",
      "Manage Domains",
      "Create and remove Asset Domains.");

  public static final Privilege MANAGE_TESTS_PRIVILEGE = Privilege.of(
      "MANAGE_TESTS",
      "Manage Tests",
      "Create and remove Asset Tests.");

  public static final Privilege MANAGE_GLOSSARIES_PRIVILEGE = Privilege.of(
      "MANAGE_GLOSSARIES",
      "Manage Glossaries",
      "Create, edit, and remove Glossary Entities");

  public static final Privilege MANAGE_USER_CREDENTIALS_PRIVILEGE =
      Privilege.of("MANAGE_USER_CREDENTIALS", "Manage User Credentials",
          "Manage credentials for native DataHub users, including inviting new users and resetting passwords");

  public static final Privilege MANAGE_TAGS_PRIVILEGE = Privilege.of(
      "MANAGE_TAGS",
      "Manage Tags",
      "Create and remove Tags.");

  public static final Privilege CREATE_TAGS_PRIVILEGE = Privilege.of(
      "CREATE_TAGS",
      "Create Tags",
      "Create new Tags.");

  public static final Privilege CREATE_DOMAINS_PRIVILEGE = Privilege.of(
      "CREATE_DOMAINS",
      "Create Domains",
      "Create new Domains.");

  public static final List<Privilege> PLATFORM_PRIVILEGES = ImmutableList.of(
      MANAGE_POLICIES_PRIVILEGE,
      MANAGE_USERS_AND_GROUPS_PRIVILEGE,
      VIEW_ANALYTICS_PRIVILEGE,
      MANAGE_DOMAINS_PRIVILEGE,
      MANAGE_INGESTION_PRIVILEGE,
      MANAGE_SECRETS_PRIVILEGE,
      GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE,
      MANAGE_ACCESS_TOKENS,
      MANAGE_TESTS_PRIVILEGE,
      MANAGE_GLOSSARIES_PRIVILEGE,
      MANAGE_USER_CREDENTIALS_PRIVILEGE,
      MANAGE_TAGS_PRIVILEGE,
      CREATE_TAGS_PRIVILEGE,
      CREATE_DOMAINS_PRIVILEGE
  );

  // Resource Privileges //

  public static final Privilege VIEW_ENTITY_PAGE_PRIVILEGE = Privilege.of(
      "VIEW_ENTITY_PAGE",
      "View Entity Page",
      "The ability to view the entity page.");

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
      "The ability to add and remove owners of an entity.");

  public static final Privilege EDIT_ENTITY_DOCS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_DOCS",
      "Edit Description",
      "The ability to edit the description (documentation) of an entity.");

  public static final Privilege EDIT_ENTITY_DOC_LINKS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_DOC_LINKS",
      "Edit Links",
      "The ability to edit links associated with an entity.");

  public static final Privilege EDIT_ENTITY_STATUS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_STATUS",
      "Edit Status",
      "The ability to edit the status of an entity (soft deleted or not).");

  public static final Privilege EDIT_ENTITY_DOMAINS_PRIVILEGE = Privilege.of(
      "EDIT_DOMAINS_PRIVILEGE",
      "Edit Domain",
      "The ability to edit the Domain of an entity.");

  public static final Privilege EDIT_ENTITY_DEPRECATION_PRIVILEGE = Privilege.of(
      "EDIT_DEPRECATION_PRIVILEGE",
      "Edit Deprecation",
      "The ability to edit the Deprecation status of an entity.");

  public static final Privilege EDIT_ENTITY_ASSERTIONS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_ASSERTIONS",
      "Edit Assertions",
      "The ability to add and remove assertions from an entity.");

  public static final Privilege EDIT_ENTITY_OPERATIONS_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY_OPERATIONS",
      "Edit Operations",
      "The ability to report or edit operations information about an entity.");

  public static final Privilege EDIT_ENTITY_PRIVILEGE = Privilege.of(
      "EDIT_ENTITY",
      "Edit All",
      "The ability to edit any information about an entity. Super user privileges.");

  public static final Privilege DELETE_ENTITY_PRIVILEGE = Privilege.of(
      "DELETE_ENTITY",
      "Delete",
      "The ability to delete the delete this entity.");

  public static final List<Privilege> COMMON_ENTITY_PRIVILEGES = ImmutableList.of(
      VIEW_ENTITY_PAGE_PRIVILEGE,
      EDIT_ENTITY_TAGS_PRIVILEGE,
      EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE,
      EDIT_ENTITY_OWNERS_PRIVILEGE,
      EDIT_ENTITY_DOCS_PRIVILEGE,
      EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
      EDIT_ENTITY_STATUS_PRIVILEGE,
      EDIT_ENTITY_DOMAINS_PRIVILEGE,
      EDIT_ENTITY_DEPRECATION_PRIVILEGE,
      EDIT_ENTITY_PRIVILEGE,
      DELETE_ENTITY_PRIVILEGE
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

  public static final Privilege VIEW_DATASET_USAGE_PRIVILEGE = Privilege.of(
      "VIEW_DATASET_USAGE",
      "View Dataset Usage",
      "The ability to access dataset usage information (includes usage statistics and queries).");

  public static final Privilege VIEW_DATASET_PROFILE_PRIVILEGE = Privilege.of(
      "VIEW_DATASET_PROFILE",
      "View Dataset Profile",
      "The ability to access dataset profile (snapshot statistics)");

  // Tag Privileges
  public static final Privilege EDIT_TAG_COLOR_PRIVILEGE = Privilege.of(
      "EDIT_TAG_COLOR",
      "Edit Tag Color",
      "The ability to change the color of a Tag.");

  // Group Privileges
  public static final Privilege EDIT_GROUP_MEMBERS_PRIVILEGE = Privilege.of(
      "EDIT_GROUP_MEMBERS",
      "Edit Group Members",
      "The ability to add and remove members to a group.");

  // User Privileges
  public static final Privilege EDIT_USER_PROFILE_PRIVILEGE = Privilege.of(
      "EDIT_USER_PROFILE",
      "Edit User Profile",
      "The ability to change the user's profile including display name, bio, title, profile image, etc.");

  // User + Group Privileges
  public static final Privilege EDIT_CONTACT_INFO_PRIVILEGE = Privilege.of(
      "EDIT_CONTACT_INFO",
      "Edit Contact Information",
      "The ability to change the contact information such as email & chat handles.");

  public static final ResourcePrivileges DATASET_PRIVILEGES = ResourcePrivileges.of(
      "dataset",
      "Datasets",
      "Datasets indexed by DataHub", Stream.of(
          COMMON_ENTITY_PRIVILEGES,
          ImmutableList.of(
              VIEW_DATASET_USAGE_PRIVILEGE,
              VIEW_DATASET_PROFILE_PRIVILEGE,
              EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE,
              EDIT_DATASET_COL_TAGS_PRIVILEGE,
              EDIT_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE,
              EDIT_ENTITY_ASSERTIONS_PRIVILEGE))
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

  // Data Doc Privileges
  public static final ResourcePrivileges NOTEBOOK_PRIVILEGES = ResourcePrivileges.of(
      "notebook",
      "Notebook",
      "Notebook indexed by DataHub",
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
      ImmutableList.of(VIEW_ENTITY_PAGE_PRIVILEGE, EDIT_ENTITY_OWNERS_PRIVILEGE, EDIT_TAG_COLOR_PRIVILEGE,
          EDIT_ENTITY_DOCS_PRIVILEGE, EDIT_ENTITY_PRIVILEGE, DELETE_ENTITY_PRIVILEGE)
  );

  // Container Privileges
  public static final ResourcePrivileges CONTAINER_PRIVILEGES = ResourcePrivileges.of(
      "container",
      "Containers",
      "Containers indexed by DataHub",
      COMMON_ENTITY_PRIVILEGES
  );

  // Domain Privileges
  public static final ResourcePrivileges DOMAIN_PRIVILEGES = ResourcePrivileges.of(
      "domain",
      "Domains",
      "Domains created on DataHub",
      ImmutableList.of(VIEW_ENTITY_PAGE_PRIVILEGE, EDIT_ENTITY_OWNERS_PRIVILEGE, EDIT_ENTITY_DOCS_PRIVILEGE,
          EDIT_ENTITY_DOC_LINKS_PRIVILEGE, EDIT_ENTITY_PRIVILEGE, DELETE_ENTITY_PRIVILEGE)
  );

  // Glossary Term Privileges
  public static final ResourcePrivileges GLOSSARY_TERM_PRIVILEGES = ResourcePrivileges.of(
      "glossaryTerm",
      "Glossary Terms",
      "Glossary Terms created on DataHub",
      ImmutableList.of(
          VIEW_ENTITY_PAGE_PRIVILEGE,
          EDIT_ENTITY_OWNERS_PRIVILEGE,
          EDIT_ENTITY_DOCS_PRIVILEGE,
          EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
          EDIT_ENTITY_DEPRECATION_PRIVILEGE,
          EDIT_ENTITY_PRIVILEGE)
  );

  // Group Privileges
  public static final ResourcePrivileges CORP_GROUP_PRIVILEGES = ResourcePrivileges.of(
      "corpGroup",
      "Groups",
      "Groups on DataHub",
      ImmutableList.of(
          VIEW_ENTITY_PAGE_PRIVILEGE,
          EDIT_ENTITY_OWNERS_PRIVILEGE,
          EDIT_GROUP_MEMBERS_PRIVILEGE,
          EDIT_CONTACT_INFO_PRIVILEGE,
          EDIT_ENTITY_DOCS_PRIVILEGE,
          EDIT_ENTITY_PRIVILEGE)
  );

  // User Privileges
  public static final ResourcePrivileges CORP_USER_PRIVILEGES = ResourcePrivileges.of(
      "corpuser",
      "Users",
      "Users on DataHub",
      ImmutableList.of(
          VIEW_ENTITY_PAGE_PRIVILEGE,
          EDIT_CONTACT_INFO_PRIVILEGE,
          EDIT_USER_PROFILE_PRIVILEGE,
          EDIT_ENTITY_PRIVILEGE)
  );

  public static final List<ResourcePrivileges> ENTITY_RESOURCE_PRIVILEGES = ImmutableList.of(
      DATASET_PRIVILEGES,
      DASHBOARD_PRIVILEGES,
      CHART_PRIVILEGES,
      DATA_FLOW_PRIVILEGES,
      DATA_JOB_PRIVILEGES,
      TAG_PRIVILEGES,
      CONTAINER_PRIVILEGES,
      DOMAIN_PRIVILEGES,
      GLOSSARY_TERM_PRIVILEGES,
      CORP_GROUP_PRIVILEGES,
      CORP_USER_PRIVILEGES,
      NOTEBOOK_PRIVILEGES
  );

  // Merge all entity specific resource privileges to create a superset of all resource privileges
  public static final ResourcePrivileges ALL_RESOURCE_PRIVILEGES = ResourcePrivileges.of(
      "all",
      "All Types",
      "All Types",
      ENTITY_RESOURCE_PRIVILEGES.stream().flatMap(resourcePrivileges -> resourcePrivileges.getPrivileges().stream()).distinct().collect(
          Collectors.toList())
  );

  public static final List<ResourcePrivileges> RESOURCE_PRIVILEGES =
      ImmutableList.<ResourcePrivileges>builder().addAll(ENTITY_RESOURCE_PRIVILEGES)
          .add(ALL_RESOURCE_PRIVILEGES)
          .build();

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
