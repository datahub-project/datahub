package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.Disjunctive.DENY_ACCESS;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.Constants;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/** This policies config file defines the base set of privileges that DataHub supports. */
public class PoliciesConfig {

  public static final String PLATFORM_POLICY_TYPE = "PLATFORM";
  public static final String METADATA_POLICY_TYPE = "METADATA";
  public static final String ACTIVE_POLICY_STATE = "ACTIVE";
  public static final String INACTIVE_POLICY_STATE = "INACTIVE";

  // Platform Privileges //

  static final Privilege MANAGE_POLICIES_PRIVILEGE =
      Privilege.of(
          "MANAGE_POLICIES",
          "Manage Policies",
          "Create and remove access control policies. Be careful - Actors with this privilege are effectively super users.");

  static final Privilege MANAGE_INGESTION_PRIVILEGE =
      Privilege.of(
          "MANAGE_INGESTION",
          "Manage Metadata Ingestion",
          "Create, remove, and update Metadata Ingestion sources.");

  static final Privilege MANAGE_SECRETS_PRIVILEGE =
      Privilege.of(
          "MANAGE_SECRETS", "Manage Secrets", "Create & remove Secrets stored inside DataHub.");

  static final Privilege MANAGE_USERS_AND_GROUPS_PRIVILEGE =
      Privilege.of(
          "MANAGE_USERS_AND_GROUPS",
          "Manage Users & Groups",
          "Create, remove, and update users and groups on DataHub.");

  static final Privilege CREATE_USERS_AND_GROUPS_PRIVILEGE =
      Privilege.of(
          "CREATE_USERS_AND_GROUPS",
          "Create Users & Groups",
          "Create users and groups on DataHub.");

  static final Privilege UPDATE_USERS_AND_GROUPS_PRIVILEGE =
      Privilege.of(
          "UPDATE_USERS_AND_GROUPS",
          "Update Users & Groups",
          "Update users and groups on DataHub.");

  private static final Privilege VIEW_ANALYTICS_PRIVILEGE =
      Privilege.of("VIEW_ANALYTICS", "View Analytics", "View the DataHub analytics dashboard.");

  private static final Privilege GET_ANALYTICS_PRIVILEGE =
      Privilege.of(
          "GET_ANALYTICS_PRIVILEGE",
          "Analytics API access",
          "API read access to raw analytics data.");

  public static final Privilege GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE =
      Privilege.of(
          "GENERATE_PERSONAL_ACCESS_TOKENS",
          "Generate Personal Access Tokens",
          "Generate personal access tokens for use with DataHub APIs.");

  public static final Privilege MANAGE_ACCESS_TOKENS =
      Privilege.of(
          "MANAGE_ACCESS_TOKENS",
          "Manage All Access Tokens",
          "Create, list and revoke access tokens on behalf of users in DataHub. Be careful - Actors with this "
              + "privilege are effectively super users that can impersonate other users.");

  public static final Privilege MANAGE_DOMAINS_PRIVILEGE =
      Privilege.of("MANAGE_DOMAINS", "Manage Domains", "Create and remove Asset Domains.");

  public static final Privilege MANAGE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE =
      Privilege.of(
          "MANAGE_GLOBAL_ANNOUNCEMENTS",
          "Manage Home Page Posts",
          "Create and delete home page posts");

  public static final Privilege VIEW_TESTS_PRIVILEGE =
      Privilege.of("VIEW_TESTS", "View Tests", "View Asset Tests.");

  public static final Privilege MANAGE_TESTS_PRIVILEGE =
      Privilege.of("MANAGE_TESTS", "Manage Tests", "Create and remove Asset Tests.");

  public static final Privilege MANAGE_GLOSSARIES_PRIVILEGE =
      Privilege.of(
          "MANAGE_GLOSSARIES", "Manage Glossaries", "Create, edit, and remove Glossary Entities");

  public static final Privilege MANAGE_USER_CREDENTIALS_PRIVILEGE =
      Privilege.of(
          "MANAGE_USER_CREDENTIALS",
          "Manage User Credentials",
          "Manage credentials for native DataHub users, including inviting new users and resetting passwords");

  public static final Privilege MANAGE_TAGS_PRIVILEGE =
      Privilege.of("MANAGE_TAGS", "Manage Tags", "Create and remove Tags.");

  public static final Privilege CREATE_TAGS_PRIVILEGE =
      Privilege.of("CREATE_TAGS", "Create Tags", "Create new Tags.");

  public static final Privilege CREATE_DOMAINS_PRIVILEGE =
      Privilege.of("CREATE_DOMAINS", "Create Domains", "Create new Domains.");

  public static final Privilege CREATE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE =
      Privilege.of(
          "CREATE_GLOBAL_ANNOUNCEMENTS",
          "Create Global Announcements",
          "Create new Global Announcements.");

  public static final Privilege MANAGE_GLOBAL_VIEWS =
      Privilege.of(
          "MANAGE_GLOBAL_VIEWS",
          "Manage Public Views",
          "Create, update, and delete any Public (shared) Views.");

  public static final Privilege MANAGE_GLOBAL_OWNERSHIP_TYPES =
      Privilege.of(
          "MANAGE_GLOBAL_OWNERSHIP_TYPES",
          "Manage Ownership Types",
          "Create, update and delete Ownership Types.");

  public static final Privilege CREATE_BUSINESS_ATTRIBUTE_PRIVILEGE =
      Privilege.of(
          "CREATE_BUSINESS_ATTRIBUTE",
          "Create Business Attribute",
          "Create new Business Attribute.");

  public static final Privilege MANAGE_BUSINESS_ATTRIBUTE_PRIVILEGE =
      Privilege.of(
          "MANAGE_BUSINESS_ATTRIBUTE",
          "Manage Business Attribute",
          "Create, update, delete Business Attribute");

  public static final Privilege MANAGE_CONNECTIONS_PRIVILEGE =
      Privilege.of(
          "MANAGE_CONNECTIONS",
          "Manage Connections",
          "Manage connections to external DataHub platforms.");

  public static final Privilege MANAGE_STRUCTURED_PROPERTIES_PRIVILEGE =
      Privilege.of(
          "MANAGE_STRUCTURED_PROPERTIES",
          "Manage Structured Properties",
          "Manage structured properties in your instance.");

  public static final Privilege VIEW_STRUCTURED_PROPERTIES_PAGE_PRIVILEGE =
      Privilege.of(
          "VIEW_STRUCTURED_PROPERTIES_PAGE",
          "View Structured Properties",
          "View structured properties in your instance.");

  public static final Privilege MANAGE_DOCUMENTATION_FORMS_PRIVILEGE =
      Privilege.of(
          "MANAGE_DOCUMENTATION_FORMS",
          "Manage Documentation Forms",
          "Manage forms assigned to assets to assist in documentation efforts.");

  public static final Privilege MANAGE_FEATURES_PRIVILEGE =
      Privilege.of(
          "MANAGE_FEATURES", "Manage Features", "Umbrella privilege to manage all features.");

  public static final Privilege MANAGE_SYSTEM_OPERATIONS_PRIVILEGE =
      Privilege.of(
          "MANAGE_SYSTEM_OPERATIONS",
          "Manage System Operations",
          "Allow access to all system operations/management APIs and controls.");

  public static final List<Privilege> PLATFORM_PRIVILEGES =
      ImmutableList.of(
          MANAGE_POLICIES_PRIVILEGE,
          MANAGE_USERS_AND_GROUPS_PRIVILEGE,
          CREATE_USERS_AND_GROUPS_PRIVILEGE,
          UPDATE_USERS_AND_GROUPS_PRIVILEGE,
          VIEW_ANALYTICS_PRIVILEGE,
          GET_ANALYTICS_PRIVILEGE,
          MANAGE_DOMAINS_PRIVILEGE,
          MANAGE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE,
          MANAGE_INGESTION_PRIVILEGE,
          MANAGE_SECRETS_PRIVILEGE,
          GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE,
          MANAGE_ACCESS_TOKENS,
          VIEW_TESTS_PRIVILEGE,
          MANAGE_TESTS_PRIVILEGE,
          MANAGE_GLOSSARIES_PRIVILEGE,
          MANAGE_USER_CREDENTIALS_PRIVILEGE,
          MANAGE_TAGS_PRIVILEGE,
          CREATE_TAGS_PRIVILEGE,
          CREATE_DOMAINS_PRIVILEGE,
          CREATE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE,
          MANAGE_GLOBAL_VIEWS,
          MANAGE_GLOBAL_OWNERSHIP_TYPES,
          CREATE_BUSINESS_ATTRIBUTE_PRIVILEGE,
          MANAGE_BUSINESS_ATTRIBUTE_PRIVILEGE,
          MANAGE_CONNECTIONS_PRIVILEGE,
          MANAGE_STRUCTURED_PROPERTIES_PRIVILEGE,
          VIEW_STRUCTURED_PROPERTIES_PAGE_PRIVILEGE,
          MANAGE_DOCUMENTATION_FORMS_PRIVILEGE,
          MANAGE_FEATURES_PRIVILEGE,
          MANAGE_SYSTEM_OPERATIONS_PRIVILEGE);

  // Resource Privileges //

  static final Privilege VIEW_ENTITY_PAGE_PRIVILEGE =
      Privilege.of("VIEW_ENTITY_PAGE", "View Entity Page", "The ability to view the entity page.");

  static final Privilege EXISTS_ENTITY_PRIVILEGE =
      Privilege.of(
          "EXISTS_ENTITY", "Entity Exists", "The ability to determine whether the entity exists.");

  static final Privilege CREATE_ENTITY_PRIVILEGE =
      Privilege.of(
          "CREATE_ENTITY", "Create Entity", "The ability to create an entity if it doesn't exist.");

  public static final Privilege EDIT_ENTITY_TAGS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_TAGS", "Edit Tags", "The ability to add and remove tags to an asset.");

  public static final Privilege EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_GLOSSARY_TERMS",
          "Edit Glossary Terms",
          "The ability to add and remove glossary terms to an asset.");

  public static final Privilege EDIT_ENTITY_OWNERS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_OWNERS",
          "Edit Owners",
          "The ability to add and remove owners of an entity.");

  public static final Privilege EDIT_ENTITY_DOCS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_DOCS",
          "Edit Description",
          "The ability to edit the description (documentation) of an entity.");

  public static final Privilege EDIT_ENTITY_DOC_LINKS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_DOC_LINKS",
          "Edit Links",
          "The ability to edit links associated with an entity.");

  public static final Privilege EDIT_ENTITY_STATUS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_STATUS",
          "Edit Status",
          "The ability to edit the status of an entity (soft deleted or not).");

  public static final Privilege EDIT_ENTITY_DOMAINS_PRIVILEGE =
      Privilege.of(
          "EDIT_DOMAINS_PRIVILEGE", "Edit Domain", "The ability to edit the Domain of an entity.");

  public static final Privilege EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_DATA_PRODUCTS",
          "Edit Data Product",
          "The ability to edit the Data Product of an entity.");

  public static final Privilege EDIT_ENTITY_DEPRECATION_PRIVILEGE =
      Privilege.of(
          "EDIT_DEPRECATION_PRIVILEGE",
          "Edit Deprecation",
          "The ability to edit the Deprecation status of an entity.");

  public static final Privilege EDIT_ENTITY_ASSERTIONS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_ASSERTIONS",
          "Edit Assertions",
          "The ability to add and remove assertions from an entity.");

  public static final Privilege EDIT_ENTITY_OPERATIONS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_OPERATIONS",
          "Edit Operations",
          "The ability to report or edit operations information about an entity.");

  public static final Privilege EDIT_ENTITY_INCIDENTS_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_INCIDENTS",
          "Edit Incidents",
          "The ability to create and remove incidents for an entity.");

  public static final Privilege EDIT_ENTITY_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY",
          "Edit Entity",
          "The ability to edit any information about an entity. Super user privileges for the entity.");

  static final Privilege DELETE_ENTITY_PRIVILEGE =
      Privilege.of("DELETE_ENTITY", "Delete", "The ability to delete this entity.");

  static final Privilege EDIT_LINEAGE_PRIVILEGE =
      Privilege.of(
          "EDIT_LINEAGE",
          "Edit Lineage",
          "The ability to add and remove lineage edges for this entity.");

  public static final Privilege EDIT_ENTITY_EMBED_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_EMBED",
          "Edit Embedded Content",
          "The ability to edit the embedded content for an entity.");

  public static final Privilege EDIT_ENTITY_PROPERTIES_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_PROPERTIES",
          "Edit Properties",
          "The ability to edit the properties for an entity.");

  public static final Privilege CREATE_ER_MODEL_RELATIONSHIP_PRIVILEGE =
      Privilege.of(
          "CREATE_ENTITY_ER_MODEL_RELATIONSHIP",
          "Create erModelRelationship",
          "The ability to add erModelRelationship on a dataset.");

  public static final List<Privilege> COMMON_ENTITY_PRIVILEGES =
      ImmutableList.of(
          VIEW_ENTITY_PAGE_PRIVILEGE,
          EDIT_ENTITY_TAGS_PRIVILEGE,
          EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE,
          EDIT_ENTITY_OWNERS_PRIVILEGE,
          EDIT_ENTITY_DOCS_PRIVILEGE,
          EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
          EDIT_ENTITY_STATUS_PRIVILEGE,
          EDIT_ENTITY_DOMAINS_PRIVILEGE,
          EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGE,
          EDIT_ENTITY_DEPRECATION_PRIVILEGE,
          EDIT_ENTITY_PRIVILEGE,
          DELETE_ENTITY_PRIVILEGE,
          EDIT_ENTITY_PROPERTIES_PRIVILEGE,
          EDIT_ENTITY_INCIDENTS_PRIVILEGE,
          CREATE_ENTITY_PRIVILEGE,
          EXISTS_ENTITY_PRIVILEGE);

  // Dataset Privileges
  public static final Privilege EDIT_DATASET_COL_TAGS_PRIVILEGE =
      Privilege.of(
          "EDIT_DATASET_COL_TAGS",
          "Edit Dataset Column Tags",
          "The ability to edit the column (field) tags associated with a dataset schema.");

  public static final Privilege EDIT_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE =
      Privilege.of(
          "EDIT_DATASET_COL_GLOSSARY_TERMS",
          "Edit Dataset Column Glossary Terms",
          "The ability to edit the column (field) glossary terms associated with a dataset schema.");

  public static final Privilege EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE =
      Privilege.of(
          "EDIT_DATASET_COL_DESCRIPTION",
          "Edit Dataset Column Descriptions",
          "The ability to edit the column (field) descriptions associated with a dataset schema.");

  public static final Privilege VIEW_DATASET_USAGE_PRIVILEGE =
      Privilege.of(
          "VIEW_DATASET_USAGE",
          "View Dataset Usage",
          "The ability to access dataset usage information (includes usage statistics and queries).");

  public static final Privilege VIEW_DATASET_PROFILE_PRIVILEGE =
      Privilege.of(
          "VIEW_DATASET_PROFILE",
          "View Dataset Profile",
          "The ability to access dataset profile (snapshot statistics)");

  public static final Privilege EDIT_QUERIES_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_QUERIES",
          "Edit Dataset Queries",
          "The ability to edit the Queries for a Dataset.");

  public static final Privilege EDIT_ENTITY_DATA_CONTRACT_PRIVILEGE =
      Privilege.of(
          "EDIT_ENTITY_DATA_CONTRACT",
          "Edit Data Contract",
          "The ability to edit the Data Contract for an entity.");

  // Tag Privileges
  public static final Privilege EDIT_TAG_COLOR_PRIVILEGE =
      Privilege.of("EDIT_TAG_COLOR", "Edit Tag Color", "The ability to change the color of a Tag.");

  // Group Privileges
  public static final Privilege EDIT_GROUP_MEMBERS_PRIVILEGE =
      Privilege.of(
          "EDIT_GROUP_MEMBERS",
          "Edit Group Members",
          "The ability to add and remove members to a group.");

  // User Privileges
  public static final Privilege EDIT_USER_PROFILE_PRIVILEGE =
      Privilege.of(
          "EDIT_USER_PROFILE",
          "Edit User Profile",
          "The ability to change the user's profile including display name, bio, title, profile image, etc.");

  // User + Group Privileges
  public static final Privilege EDIT_CONTACT_INFO_PRIVILEGE =
      Privilege.of(
          "EDIT_CONTACT_INFO",
          "Edit Contact Information",
          "The ability to change the contact information such as email & chat handles.");

  // Glossary Node Privileges
  public static final Privilege MANAGE_GLOSSARY_CHILDREN_PRIVILEGE =
      Privilege.of(
          "MANAGE_GLOSSARY_CHILDREN",
          "Manage Direct Glossary Children",
          "The ability to create and delete the direct children of this entity.");

  // Glossary Node Privileges
  public static final Privilege MANAGE_ALL_GLOSSARY_CHILDREN_PRIVILEGE =
      Privilege.of(
          "MANAGE_ALL_GLOSSARY_CHILDREN",
          "Manage All Glossary Children",
          "The ability to create and delete everything underneath this entity.");

  // REST API Specific Privileges (not adding to lists of privileges above as those affect GraphQL
  // as well)
  public static final Privilege GET_TIMELINE_PRIVILEGE =
      Privilege.of(
          "GET_TIMELINE_PRIVILEGE", "Get Timeline API", "The ability to use the GET Timeline API.");

  static final Privilege GET_ENTITY_PRIVILEGE =
      Privilege.of(
          "GET_ENTITY_PRIVILEGE",
          "Get Entity + Relationships API",
          "The ability to use the GET Entity and Relationships API.");

  static final Privilege GET_TIMESERIES_ASPECT_PRIVILEGE =
      Privilege.of(
          "GET_TIMESERIES_ASPECT_PRIVILEGE",
          "Get Timeseries Aspect API",
          "The ability to use the GET Timeseries Aspect API.");

  static final Privilege GET_COUNTS_PRIVILEGE =
      Privilege.of(
          "GET_COUNTS_PRIVILEGE",
          "Get Aspect/Entity Count APIs",
          "The ability to use the GET Aspect/Entity Count APIs.");

  public static final Privilege RESTORE_INDICES_PRIVILEGE =
      Privilege.of(
          "RESTORE_INDICES_PRIVILEGE",
          "Restore Indices API",
          "The ability to use the Restore Indices API.");

  public static final Privilege GET_TIMESERIES_INDEX_SIZES_PRIVILEGE =
      Privilege.of(
          "GET_TIMESERIES_INDEX_SIZES_PRIVILEGE",
          "Get Timeseries index sizes API",
          "The ability to use the get Timeseries indices size API.");

  public static final Privilege TRUNCATE_TIMESERIES_INDEX_PRIVILEGE =
      Privilege.of(
          "TRUNCATE_TIMESERIES_INDEX_PRIVILEGE",
          "Truncate timeseries aspect index size API",
          "The ability to use the API to truncate a timeseries index.");

  public static final Privilege GET_ES_TASK_STATUS_PRIVILEGE =
      Privilege.of(
          "GET_ES_TASK_STATUS_PRIVILEGE",
          "Get ES task status API",
          "The ability to use the get task status API for an ElasticSearch task.");

  public static final Privilege ES_EXPLAIN_QUERY_PRIVILEGE =
      Privilege.of(
          "ES_EXPLAIN_QUERY_PRIVILEGE",
          "Explain ElasticSearch Query API",
          "The ability to use the Operations API explain endpoint.");

  /* Prefer per entity READ */
  @Deprecated
  static final Privilege SEARCH_PRIVILEGE =
      Privilege.of("SEARCH_PRIVILEGE", "Search API", "The ability to access search APIs.");

  public static final Privilege SET_WRITEABLE_PRIVILEGE =
      Privilege.of(
          "SET_WRITEABLE_PRIVILEGE",
          "Enable/Disable Writeability API",
          "The ability to enable or disable GMS writeability for data migrations.");

  public static final Privilege APPLY_RETENTION_PRIVILEGE =
      Privilege.of(
          "APPLY_RETENTION_PRIVILEGE",
          "Apply Retention API",
          "The ability to apply retention using the API.");

  public static final Privilege PRODUCE_PLATFORM_EVENT_PRIVILEGE =
      Privilege.of(
          "PRODUCE_PLATFORM_EVENT_PRIVILEGE",
          "Produce Platform Event API",
          "The ability to produce Platform Events using the API.");

  public static final ResourcePrivileges DATASET_PRIVILEGES =
      ResourcePrivileges.of(
          "dataset",
          "Datasets",
          "Datasets indexed by DataHub",
          Stream.of(
                  COMMON_ENTITY_PRIVILEGES,
                  ImmutableList.of(
                      VIEW_DATASET_USAGE_PRIVILEGE,
                      VIEW_DATASET_PROFILE_PRIVILEGE,
                      EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE,
                      EDIT_DATASET_COL_TAGS_PRIVILEGE,
                      EDIT_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE,
                      EDIT_ENTITY_ASSERTIONS_PRIVILEGE,
                      EDIT_LINEAGE_PRIVILEGE,
                      EDIT_ENTITY_EMBED_PRIVILEGE,
                      EDIT_QUERIES_PRIVILEGE,
                      CREATE_ER_MODEL_RELATIONSHIP_PRIVILEGE))
              .flatMap(Collection::stream)
              .collect(Collectors.toList()));

  // Charts Privileges
  public static final ResourcePrivileges CHART_PRIVILEGES =
      ResourcePrivileges.of(
          "chart",
          "Charts",
          "Charts indexed by DataHub",
          Stream.concat(
                  COMMON_ENTITY_PRIVILEGES.stream(),
                  ImmutableList.of(EDIT_LINEAGE_PRIVILEGE, EDIT_ENTITY_EMBED_PRIVILEGE).stream())
              .collect(Collectors.toList()));

  // Dashboard Privileges
  public static final ResourcePrivileges DASHBOARD_PRIVILEGES =
      ResourcePrivileges.of(
          "dashboard",
          "Dashboards",
          "Dashboards indexed by DataHub",
          Stream.concat(
                  COMMON_ENTITY_PRIVILEGES.stream(),
                  ImmutableList.of(EDIT_LINEAGE_PRIVILEGE, EDIT_ENTITY_EMBED_PRIVILEGE).stream())
              .collect(Collectors.toList()));

  // Data Doc Privileges
  public static final ResourcePrivileges NOTEBOOK_PRIVILEGES =
      ResourcePrivileges.of(
          "notebook", "Notebook", "Notebook indexed by DataHub", COMMON_ENTITY_PRIVILEGES);

  // Data Flow Privileges
  public static final ResourcePrivileges DATA_FLOW_PRIVILEGES =
      ResourcePrivileges.of(
          "dataFlow",
          "Data Pipelines",
          "Data Pipelines indexed by DataHub",
          COMMON_ENTITY_PRIVILEGES);

  // Data Job Privileges
  public static final ResourcePrivileges DATA_JOB_PRIVILEGES =
      ResourcePrivileges.of(
          "dataJob",
          "Data Tasks",
          "Data Tasks indexed by DataHub",
          Stream.concat(
                  COMMON_ENTITY_PRIVILEGES.stream(),
                  ImmutableList.of(EDIT_LINEAGE_PRIVILEGE).stream())
              .collect(Collectors.toList()));

  // Tag Privileges
  public static final ResourcePrivileges TAG_PRIVILEGES =
      ResourcePrivileges.of(
          "tag",
          "Tags",
          "Tags indexed by DataHub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_ENTITY_OWNERS_PRIVILEGE,
              EDIT_TAG_COLOR_PRIVILEGE,
              EDIT_ENTITY_DOCS_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              DELETE_ENTITY_PRIVILEGE,
              CREATE_ENTITY_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE));

  // Container Privileges
  public static final ResourcePrivileges CONTAINER_PRIVILEGES =
      ResourcePrivileges.of(
          "container", "Containers", "Containers indexed by DataHub", COMMON_ENTITY_PRIVILEGES);

  // Domain Privileges
  public static final Privilege MANAGE_DATA_PRODUCTS_PRIVILEGE =
      Privilege.of(
          "MANAGE_DATA_PRODUCTS",
          "Manage Data Products",
          "The ability to create, edit, and delete Data Products within a Domain");

  // Domain Privileges
  public static final ResourcePrivileges DOMAIN_PRIVILEGES =
      ResourcePrivileges.of(
          "domain",
          "Domains",
          "Domains created on DataHub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_ENTITY_OWNERS_PRIVILEGE,
              EDIT_ENTITY_DOCS_PRIVILEGE,
              EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              DELETE_ENTITY_PRIVILEGE,
              MANAGE_DATA_PRODUCTS_PRIVILEGE,
              EDIT_ENTITY_PROPERTIES_PRIVILEGE,
              CREATE_ENTITY_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE));

  // Data Product Privileges
  public static final ResourcePrivileges DATA_PRODUCT_PRIVILEGES =
      ResourcePrivileges.of(
          "dataProduct",
          "Data Products",
          "Data Products created on DataHub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_ENTITY_OWNERS_PRIVILEGE,
              EDIT_ENTITY_DOCS_PRIVILEGE,
              EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              DELETE_ENTITY_PRIVILEGE,
              EDIT_ENTITY_TAGS_PRIVILEGE,
              EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE,
              EDIT_ENTITY_DOMAINS_PRIVILEGE,
              EDIT_ENTITY_PROPERTIES_PRIVILEGE,
              CREATE_ENTITY_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE));

  // Glossary Term Privileges
  public static final ResourcePrivileges GLOSSARY_TERM_PRIVILEGES =
      ResourcePrivileges.of(
          "glossaryTerm",
          "Glossary Terms",
          "Glossary Terms created on DataHub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_ENTITY_OWNERS_PRIVILEGE,
              EDIT_ENTITY_DOCS_PRIVILEGE,
              EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
              EDIT_ENTITY_DEPRECATION_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              EDIT_ENTITY_PROPERTIES_PRIVILEGE,
              CREATE_ENTITY_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE));

  // Glossary Node Privileges
  public static final ResourcePrivileges GLOSSARY_NODE_PRIVILEGES =
      ResourcePrivileges.of(
          "glossaryNode",
          "Glossary Term Groups",
          "Glossary Term Groups created on DataHub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_ENTITY_OWNERS_PRIVILEGE,
              EDIT_ENTITY_DOCS_PRIVILEGE,
              EDIT_ENTITY_DOC_LINKS_PRIVILEGE,
              EDIT_ENTITY_DEPRECATION_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              MANAGE_GLOSSARY_CHILDREN_PRIVILEGE,
              MANAGE_ALL_GLOSSARY_CHILDREN_PRIVILEGE,
              EDIT_ENTITY_PROPERTIES_PRIVILEGE,
              CREATE_ENTITY_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE));

  // Group Privileges
  public static final ResourcePrivileges CORP_GROUP_PRIVILEGES =
      ResourcePrivileges.of(
          "corpGroup",
          "Groups",
          "Groups on DataHub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_ENTITY_OWNERS_PRIVILEGE,
              EDIT_GROUP_MEMBERS_PRIVILEGE,
              EDIT_CONTACT_INFO_PRIVILEGE,
              EDIT_ENTITY_DOCS_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              EDIT_ENTITY_PROPERTIES_PRIVILEGE,
              CREATE_ENTITY_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE));

  // User Privileges
  public static final ResourcePrivileges CORP_USER_PRIVILEGES =
      ResourcePrivileges.of(
          "corpuser",
          "Users",
          "Users on DataHub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_CONTACT_INFO_PRIVILEGE,
              EDIT_USER_PROFILE_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              EDIT_ENTITY_PROPERTIES_PRIVILEGE,
              CREATE_ENTITY_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE));

  // Properties Privileges
  public static final ResourcePrivileges STRUCTURED_PROPERTIES_PRIVILEGES =
      ResourcePrivileges.of(
          "structuredProperty",
          "Structured Properties",
          "Structured Properties",
          ImmutableList.of(
              CREATE_ENTITY_PRIVILEGE,
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EXISTS_ENTITY_PRIVILEGE,
              EDIT_ENTITY_PRIVILEGE,
              DELETE_ENTITY_PRIVILEGE));

  // ERModelRelationship Privileges
  public static final ResourcePrivileges ER_MODEL_RELATIONSHIP_PRIVILEGES =
      ResourcePrivileges.of(
          "erModelRelationship",
          "ERModelRelationship",
          "update privileges for ermodelrelations",
          COMMON_ENTITY_PRIVILEGES);
  public static final ResourcePrivileges BUSINESS_ATTRIBUTE_PRIVILEGES =
      ResourcePrivileges.of(
          "businessAttribute",
          "Business Attribute",
          "Business Attribute created on Datahub",
          ImmutableList.of(
              VIEW_ENTITY_PAGE_PRIVILEGE,
              EDIT_ENTITY_OWNERS_PRIVILEGE,
              EDIT_ENTITY_DOCS_PRIVILEGE,
              EDIT_ENTITY_TAGS_PRIVILEGE,
              EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE));

  // Version Set privileges
  public static final ResourcePrivileges VERSION_SET_PRIVILEGES =
      ResourcePrivileges.of(
          "versionSet",
          "Version Set",
          "A logical collection of versioned entities.",
          COMMON_ENTITY_PRIVILEGES);

  public static final List<ResourcePrivileges> ENTITY_RESOURCE_PRIVILEGES =
      ImmutableList.of(
          DATASET_PRIVILEGES,
          DASHBOARD_PRIVILEGES,
          CHART_PRIVILEGES,
          DATA_FLOW_PRIVILEGES,
          DATA_JOB_PRIVILEGES,
          TAG_PRIVILEGES,
          CONTAINER_PRIVILEGES,
          DOMAIN_PRIVILEGES,
          GLOSSARY_TERM_PRIVILEGES,
          GLOSSARY_NODE_PRIVILEGES,
          CORP_GROUP_PRIVILEGES,
          CORP_USER_PRIVILEGES,
          NOTEBOOK_PRIVILEGES,
          DATA_PRODUCT_PRIVILEGES,
          ER_MODEL_RELATIONSHIP_PRIVILEGES,
          BUSINESS_ATTRIBUTE_PRIVILEGES,
          STRUCTURED_PROPERTIES_PRIVILEGES,
          VERSION_SET_PRIVILEGES);

  // Merge all entity specific resource privileges to create a superset of all resource privileges
  public static final ResourcePrivileges ALL_RESOURCE_PRIVILEGES =
      ResourcePrivileges.of(
          "all",
          "All Types",
          "All Types",
          ENTITY_RESOURCE_PRIVILEGES.stream()
              .flatMap(resourcePrivileges -> resourcePrivileges.getPrivileges().stream())
              .distinct()
              .collect(Collectors.toList()));

  public static final List<ResourcePrivileges> RESOURCE_PRIVILEGES =
      ImmutableList.<ResourcePrivileges>builder()
          .addAll(ENTITY_RESOURCE_PRIVILEGES)
          .add(ALL_RESOURCE_PRIVILEGES)
          .build();

  /*
   * This is an attempt to piece together and organize CRUD-like semantics from the various existing
   * privileges. These are intended to govern lower level APIs to which CRUD semantics apply.
   *
   * These are collections of disjoint privileges, meaning a single privilege will grant access to
   * the operation.
   */
  public static Map<ApiGroup, Map<ApiOperation, Disjunctive<Conjunctive<Privilege>>>>
      API_PRIVILEGE_MAP =
          ImmutableMap.<ApiGroup, Map<ApiOperation, Disjunctive<Conjunctive<Privilege>>>>builder()
              .put(
                  ApiGroup.ENTITY,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(
                          ApiOperation.CREATE,
                          Disjunctive.disjoint(CREATE_ENTITY_PRIVILEGE, EDIT_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          Disjunctive.disjoint(
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              GET_ENTITY_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE))
                      .put(ApiOperation.UPDATE, Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE))
                      .put(ApiOperation.DELETE, Disjunctive.disjoint(DELETE_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          Disjunctive.disjoint(
                              EXISTS_ENTITY_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE,
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              SEARCH_PRIVILEGE))
                      .build())
              .put(
                  ApiGroup.LINEAGE,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(
                          ApiOperation.CREATE,
                          Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE, EDIT_LINEAGE_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          Disjunctive.disjoint(
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              GET_ENTITY_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              EDIT_LINEAGE_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.UPDATE,
                          Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE, EDIT_LINEAGE_PRIVILEGE))
                      .put(
                          ApiOperation.DELETE,
                          Disjunctive.disjoint(
                              EDIT_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE,
                              EDIT_LINEAGE_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          Disjunctive.disjoint(
                              EXISTS_ENTITY_PRIVILEGE,
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              SEARCH_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              EDIT_LINEAGE_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE,
                              GET_ENTITY_PRIVILEGE))
                      .build())
              .put(
                  ApiGroup.RELATIONSHIP,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(ApiOperation.CREATE, Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          Disjunctive.disjoint(
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              GET_ENTITY_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE))
                      .put(ApiOperation.UPDATE, Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.DELETE,
                          Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE, DELETE_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          Disjunctive.disjoint(
                              EXISTS_ENTITY_PRIVILEGE,
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              SEARCH_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE,
                              GET_ENTITY_PRIVILEGE))
                      .build())
              .put(
                  ApiGroup.ANALYTICS,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(ApiOperation.CREATE, DENY_ACCESS)
                      .put(
                          ApiOperation.READ,
                          Disjunctive.disjoint(
                              VIEW_ANALYTICS_PRIVILEGE,
                              GET_ANALYTICS_PRIVILEGE,
                              MANAGE_SYSTEM_OPERATIONS_PRIVILEGE))
                      .put(ApiOperation.UPDATE, DENY_ACCESS)
                      .put(ApiOperation.DELETE, DENY_ACCESS)
                      .put(
                          ApiOperation.EXISTS,
                          Disjunctive.disjoint(
                              VIEW_ANALYTICS_PRIVILEGE,
                              GET_ANALYTICS_PRIVILEGE,
                              SEARCH_PRIVILEGE,
                              MANAGE_SYSTEM_OPERATIONS_PRIVILEGE))
                      .build())
              .put(
                  ApiGroup.TIMESERIES,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(ApiOperation.CREATE, Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          Disjunctive.disjoint(
                              GET_TIMESERIES_ASPECT_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE))
                      .put(ApiOperation.UPDATE, Disjunctive.disjoint(EDIT_ENTITY_PRIVILEGE))
                      .put(ApiOperation.DELETE, Disjunctive.disjoint(DELETE_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          Disjunctive.disjoint(
                              EXISTS_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE,
                              SEARCH_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              GET_TIMESERIES_ASPECT_PRIVILEGE,
                              VIEW_ENTITY_PAGE_PRIVILEGE))
                      .build())
              .put(
                  ApiGroup.COUNTS,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(ApiOperation.CREATE, DENY_ACCESS)
                      .put(ApiOperation.READ, Disjunctive.disjoint(GET_COUNTS_PRIVILEGE))
                      .put(ApiOperation.UPDATE, DENY_ACCESS)
                      .put(ApiOperation.DELETE, DENY_ACCESS)
                      .put(ApiOperation.EXISTS, DENY_ACCESS)
                      .build())
              .build();

  /** Contains entity specific privileges, default to map above for non-specific entities */
  public static final Map<String, Map<ApiOperation, Disjunctive<Conjunctive<Privilege>>>>
      API_ENTITY_PRIVILEGE_MAP =
          ImmutableMap.<String, Map<ApiOperation, Disjunctive<Conjunctive<Privilege>>>>builder()
              .put(
                  Constants.POLICY_ENTITY_NAME,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(ApiOperation.CREATE, Disjunctive.disjoint(MANAGE_POLICIES_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.READ))
                      .put(ApiOperation.UPDATE, Disjunctive.disjoint(MANAGE_POLICIES_PRIVILEGE))
                      .put(ApiOperation.DELETE, Disjunctive.disjoint(MANAGE_POLICIES_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.EXISTS))
                      .build())
              .put(
                  Constants.CORP_USER_ENTITY_NAME,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(
                          ApiOperation.CREATE,
                          Disjunctive.disjoint(
                              CREATE_USERS_AND_GROUPS_PRIVILEGE, MANAGE_USERS_AND_GROUPS_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.READ))
                      .put(
                          ApiOperation.UPDATE,
                          Disjunctive.disjoint(
                              UPDATE_USERS_AND_GROUPS_PRIVILEGE, MANAGE_USERS_AND_GROUPS_PRIVILEGE))
                      .put(
                          ApiOperation.DELETE,
                          Disjunctive.disjoint(MANAGE_USERS_AND_GROUPS_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.EXISTS))
                      .build())
              .put(
                  Constants.CORP_GROUP_ENTITY_NAME,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(
                          ApiOperation.CREATE,
                          Disjunctive.disjoint(
                              CREATE_USERS_AND_GROUPS_PRIVILEGE, MANAGE_USERS_AND_GROUPS_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.READ))
                      .put(
                          ApiOperation.UPDATE,
                          Disjunctive.disjoint(
                              UPDATE_USERS_AND_GROUPS_PRIVILEGE, MANAGE_USERS_AND_GROUPS_PRIVILEGE))
                      .put(
                          ApiOperation.DELETE,
                          Disjunctive.disjoint(MANAGE_USERS_AND_GROUPS_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.EXISTS))
                      .build())
              .put(
                  Constants.INGESTION_SOURCE_ENTITY_NAME,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(ApiOperation.CREATE, Disjunctive.disjoint(MANAGE_INGESTION_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.READ))
                      .put(ApiOperation.UPDATE, Disjunctive.disjoint(MANAGE_INGESTION_PRIVILEGE))
                      .put(ApiOperation.DELETE, Disjunctive.disjoint(MANAGE_INGESTION_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.EXISTS))
                      .build())
              .put(
                  Constants.SECRETS_ENTITY_NAME,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(ApiOperation.CREATE, Disjunctive.disjoint(MANAGE_SECRETS_PRIVILEGE))
                      .put(ApiOperation.READ, Disjunctive.disjoint(MANAGE_SECRETS_PRIVILEGE))
                      .put(ApiOperation.UPDATE, Disjunctive.disjoint(MANAGE_SECRETS_PRIVILEGE))
                      .put(ApiOperation.DELETE, Disjunctive.disjoint(MANAGE_SECRETS_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          API_PRIVILEGE_MAP.get(ApiGroup.ENTITY).get(ApiOperation.EXISTS))
                      .build())
              .put(
                  // regular entity level permissions + MANAGE_ACCESS_TOKENS
                  Constants.ACCESS_TOKEN_ENTITY_NAME,
                  ImmutableMap.<ApiOperation, Disjunctive<Conjunctive<Privilege>>>builder()
                      .put(
                          ApiOperation.CREATE,
                          Disjunctive.disjoint(
                              MANAGE_ACCESS_TOKENS, CREATE_ENTITY_PRIVILEGE, EDIT_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.READ,
                          Disjunctive.disjoint(
                              MANAGE_ACCESS_TOKENS,
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              GET_ENTITY_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.UPDATE,
                          Disjunctive.disjoint(MANAGE_ACCESS_TOKENS, EDIT_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.DELETE,
                          Disjunctive.disjoint(MANAGE_ACCESS_TOKENS, DELETE_ENTITY_PRIVILEGE))
                      .put(
                          ApiOperation.EXISTS,
                          Disjunctive.disjoint(
                              MANAGE_ACCESS_TOKENS,
                              EXISTS_ENTITY_PRIVILEGE,
                              EDIT_ENTITY_PRIVILEGE,
                              DELETE_ENTITY_PRIVILEGE,
                              VIEW_ENTITY_PAGE_PRIVILEGE,
                              SEARCH_PRIVILEGE))
                      .build())
              .build();

  /**
   * Define default policies for given actors. Typically used to define allow privileges on self in
   * a common way across APIs
   *
   * @param actorUrn
   * @return
   */
  public static List<DataHubPolicyInfo> getDefaultPolicies(Urn actorUrn) {
    return ImmutableList.<DataHubPolicyInfo>builder()
        .add(
            new DataHubPolicyInfo()
                .setDisplayName("View Self")
                .setDescription("View self entity page.")
                .setActors(new DataHubActorFilter().setUsers(new UrnArray(actorUrn)))
                .setPrivileges(
                    PoliciesConfig.API_PRIVILEGE_MAP.get(ENTITY).get(READ).stream()
                        .flatMap(Collection::stream)
                        .map(PoliciesConfig.Privilege::getType)
                        .collect(Collectors.toCollection(StringArray::new)))
                .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                .setEditable(false)
                .setResources(
                    new DataHubResourceFilter()
                        .setFilter(
                            new PolicyMatchFilter()
                                .setCriteria(
                                    new PolicyMatchCriterionArray(
                                        List.of(
                                            new PolicyMatchCriterion()
                                                .setField("URN")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(
                                                    new StringArray(
                                                        List.of(actorUrn.toString())))))))))
        .build();
  }

  @Data
  @Getter
  @AllArgsConstructor
  public static class Privilege {
    @Nonnull private String type;
    private String displayName;
    private String description;

    static Privilege of(String type, String displayName, String description) {
      return new Privilege(type, displayName, description);
    }

    // Not using displayName or description in equality by design
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Privilege privilege = (Privilege) o;

      return type.equals(privilege.type);
    }

    @Override
    public int hashCode() {
      return type.hashCode();
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
      return new ResourcePrivileges(
          resourceType, resourceTypeDisplayName, resourceTypeDescription, privileges);
    }
  }

  private PoliciesConfig() {}
}
