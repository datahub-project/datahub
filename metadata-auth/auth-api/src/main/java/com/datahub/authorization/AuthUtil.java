package com.datahub.authorization;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.WILDCARD_URN;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static com.linkedin.metadata.authorization.Disjunctive.DENY_ACCESS;
import static com.linkedin.metadata.authorization.PoliciesConfig.API_ENTITY_PRIVILEGE_MAP;
import static com.linkedin.metadata.authorization.PoliciesConfig.API_PRIVILEGE_MAP;
import static com.linkedin.metadata.authorization.PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.authorization.ApiGroup;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.Conjunctive;
import com.linkedin.metadata.authorization.Disjunctive;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import jakarta.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.HttpStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Notes: This class is an attempt to unify privilege checks across APIs.
 *
 * <p>Public: The intent is that the public interface uses the typical abstractions for Urns,
 * ApiOperation, ApiGroup, and entity type strings
 *
 * <p>Private functions can use the more specific Privileges, Disjunctive/Conjunctive interfaces
 * required for the policy engine and authorizer
 *
 * <p>isAPI...() functions are intended for OpenAPI and Rest.li since they are governed by an enable
 * flag. GraphQL is always enabled and should use is...() functions.
 */
@Component
// TODO: Condense abstractions here, should ideally be on public entrypoint here with an Auth
// Request Wrapper to reduce
//       complexity of tracking flows
public class AuthUtil {

  // Since all methods of this class are static, need to postConstruct to initialize the static var
  // from the instance var that spring can initialize
  // TODO: Some unit tests seem to rely on this being false, so setting the default to false.
  // When running as the spring boot application, the default property value is true.
  private static boolean isRestApiAuthorizationEnabled = false;

  // Eliminating the dependency on the env var REST_API_AUTHORIZATION_ENABLED and instead using the
  // application property to keep it consistent with all other usage of that property.
  @Value("${authorization.restApiAuthorization:true}")
  protected Boolean restApiAuthorizationEnabled;

  @PostConstruct
  protected void init() {
    AuthUtil.isRestApiAuthorizationEnabled = this.restApiAuthorizationEnabled;
  }

  public static boolean isRestApiAuthorizationEnabled() {
    return isRestApiAuthorizationEnabled;
  }

  // getEffectiveUnrestrictedEntityTypes() is Lombok-generated, so javadoc (which does not run
  // annotation processors) cannot resolve it: {@link} here fails the auth-api javadoc task.
  /**
   * Effective view-unrestricted entity types for a request. Resolved once onto {@code
   * com.datahub.authorization.config.ViewAuthorizationConfiguration#getEffectiveUnrestrictedEntityTypes()}
   * from {@code authorization.view.unrestrictedEntityTypes} (YAML/env). When unset, returns an
   * empty set — with view authorization enabled, every entity type is restricted.
   */
  @Nonnull
  public static Set<String> getViewUnrestrictedEntityTypes(
      @Nullable com.datahub.authorization.config.ViewAuthorizationConfiguration config) {
    if (config != null && config.getEffectiveUnrestrictedEntityTypes() != null) {
      return config.getEffectiveUnrestrictedEntityTypes();
    }
    return Set.of();
  }

  /**
   * Whether {@code entityType} is subject to view authorization checks. When view authorization is
   * disabled this is unused by callers; when enabled, returns {@code true} unless the type is in
   * the unrestricted set (case-insensitive).
   */
  public static boolean isViewRestrictedEntityType(
      @Nullable com.datahub.authorization.config.ViewAuthorizationConfiguration config,
      @Nonnull String entityType) {
    final String needle = entityType.toLowerCase(java.util.Locale.ROOT);
    return getViewUnrestrictedEntityTypes(config).stream()
        .noneMatch(name -> name.toLowerCase(java.util.Locale.ROOT).equals(needle));
  }

  /** OpenAPI/Rest.li Methods */
  public static List<Pair<MetadataChangeProposal, Integer>> isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<MetadataChangeProposal> mcps) {
    return isAPIAuthorized(session, apiGroup, entityRegistry, mcps, null);
  }

  /**
   * Authorize MCPs with optional entity-existence awareness. When {@code entityExists} is provided,
   * UPSERT/UPDATE/RESTATE against a non-existent entity use the CREATE privilege path ({@code
   * CREATE_ENTITY}), and CREATE_ENTITY against an existing entity uses the UPDATE privilege path
   * ({@code EDIT_ENTITY}) so create-only callers cannot overwrite. PATCH always uses UPDATE ({@code
   * EDIT_ENTITY}), never CREATE.
   */
  public static List<Pair<MetadataChangeProposal, Integer>> isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<MetadataChangeProposal> mcps,
      @Nullable final Map<Urn, Boolean> entityExists) {

    List<Pair<Pair<ChangeType, Urn>, MetadataChangeProposal>> changeUrnMCPs =
        mcps.stream()
            .map(
                mcp -> {
                  Urn urn = mcp.getEntityUrn();
                  if (urn == null) {
                    com.linkedin.metadata.models.EntitySpec entitySpec =
                        entityRegistry.getEntitySpec(mcp.getEntityType());
                    urn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());
                  }
                  return Pair.of(Pair.of(mcp.getChangeType(), urn), mcp);
                })
            .collect(Collectors.toList());

    Map<Pair<ChangeType, Urn>, Integer> authorizationResult =
        isAPIAuthorizedUrns(
            session,
            apiGroup,
            changeUrnMCPs.stream().map(Pair::getFirst).collect(Collectors.toSet()),
            entityExists);

    return changeUrnMCPs.stream()
        .map(
            changeUrnMCP ->
                Pair.of(
                    changeUrnMCP.getValue(),
                    authorizationResult.getOrDefault(
                        changeUrnMCP.getKey(), HttpStatus.SC_INTERNAL_SERVER_ERROR)))
        .collect(Collectors.toList());
  }

  public static Map<Pair<ChangeType, Urn>, Integer> isAPIAuthorizedUrns(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final Collection<Pair<ChangeType, Urn>> changeTypeUrns) {
    return isAPIAuthorizedUrns(session, apiGroup, changeTypeUrns, null);
  }

  /**
   * @param entityExists optional map of URN → whether the entity already exists (key aspect
   *     present). When null, behavior matches the historical change-type-only privilege mapping.
   *     When non-null, only URNs present as keys use existence-aware remapping; a missing key falls
   *     back to the historical change-type-only mapping for that URN (does not treat absence as
   *     "does not exist").
   */
  public static Map<Pair<ChangeType, Urn>, Integer> isAPIAuthorizedUrns(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final Collection<Pair<ChangeType, Urn>> changeTypeUrns,
      @Nullable final Map<Urn, Boolean> entityExists) {

    return changeTypeUrns.stream()
        .distinct()
        .map(
            changeTypePair -> {
              final Urn urn = changeTypePair.getSecond();
              final boolean existenceKnown = entityExists != null && entityExists.containsKey(urn);
              final boolean exists = existenceKnown && Boolean.TRUE.equals(entityExists.get(urn));
              switch (changeTypePair.getFirst()) {
                case CREATE:
                  // Aspect-level create-if-not-exists: privilege remains EDIT_ENTITY.
                  if (!isAPIAuthorized(
                      session,
                      lookupAPIPrivilege(apiGroup, UPDATE, urn.getEntityType()),
                      new EntitySpec(urn.getEntityType(), urn.toString()),
                      Collections.emptyList())) {
                    return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                  }
                  break;
                case UPSERT:
                case UPDATE:
                case RESTATE:
                  {
                    final ApiOperation apiOperation = existenceKnown && !exists ? CREATE : UPDATE;
                    if (!isAPIAuthorized(
                        session,
                        lookupAPIPrivilege(apiGroup, apiOperation, urn.getEntityType()),
                        new EntitySpec(urn.getEntityType(), urn.toString()),
                        Collections.emptyList())) {
                      return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                    }
                    break;
                  }
                case PATCH:
                  // PATCH never uses CREATE_ENTITY — always Edit Entity.
                  if (!isAPIAuthorized(
                      session,
                      lookupAPIPrivilege(apiGroup, UPDATE, urn.getEntityType()),
                      new EntitySpec(urn.getEntityType(), urn.toString()),
                      Collections.emptyList())) {
                    return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                  }
                  break;
                case CREATE_ENTITY:
                  {
                    // Entity must not exist for CREATE_ENTITY privilege; if it exists require EDIT.
                    final ApiOperation apiOperation = existenceKnown && exists ? UPDATE : CREATE;
                    if (!isAPIAuthorized(
                        session,
                        lookupAPIPrivilege(apiGroup, apiOperation, urn.getEntityType()),
                        new EntitySpec(urn.getEntityType(), urn.toString()),
                        Collections.emptyList())) {
                      return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                    }
                    break;
                  }
                case DELETE:
                  if (!isAPIAuthorized(
                      session,
                      lookupAPIPrivilege(apiGroup, DELETE, urn.getEntityType()),
                      new EntitySpec(urn.getEntityType(), urn.toString()),
                      Collections.emptyList())) {
                    return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                  }
                  break;
                default:
                  return Pair.of(changeTypePair, HttpStatus.SC_BAD_REQUEST);
              }
              return Pair.of(changeTypePair, HttpStatus.SC_OK);
            })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull final AuthorizationSession session, @Nonnull final SearchResult result) {
    return isAPIAuthorizedEntityUrns(
        session,
        READ,
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull final AuthorizationSession session, @Nonnull final ScrollResult result) {
    return isAPIAuthorizedEntityUrns(
        session,
        READ,
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull final AuthorizationSession session, @Nonnull final AutoCompleteResult result) {
    return isAPIAuthorizedEntityUrns(
        session,
        READ,
        result.getEntities().stream().map(AutoCompleteEntity::getUrn).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull final AuthorizationSession session, @Nonnull final BrowseResult result) {
    return isAPIAuthorizedEntityUrns(
        session,
        READ,
        result.getEntities().stream().map(BrowseResultEntity::getUrn).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedUrns(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<Urn> urns) {

    if (ApiGroup.ENTITY.equals(apiGroup)) {
      return isAPIAuthorizedEntityUrns(session, apiOperation, urns);
    }

    List<EntitySpec> resourceSpecs =
        urns.stream()
            .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
            .collect(Collectors.toList());

    return isAPIAuthorized(
        session,
        lookupAPIPrivilege(apiGroup, apiOperation, null),
        resourceSpecs,
        Collections.emptyList());
  }

  public static boolean isAPIAuthorizedEntityUrns(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<Urn> urns) {

    Map<String, List<EntitySpec>> resourceSpecs =
        urns.stream()
            .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
            .collect(Collectors.groupingBy(EntitySpec::getType));

    return resourceSpecs.entrySet().stream()
        .allMatch(
            entry ->
                isAPIAuthorized(
                    session,
                    lookupAPIPrivilege(ENTITY, apiOperation, entry.getKey()),
                    entry.getValue(),
                    Collections.emptyList()));
  }

  public static boolean isAPIAuthorizedEntityUrnsWithSubResources(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<Urn> urns,
      @Nonnull final Collection<Urn> subResources) {

    Map<String, List<EntitySpec>> resourceSpecs =
        urns.stream()
            .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
            .collect(Collectors.groupingBy(EntitySpec::getType));

    Set<EntitySpec> subResourceSpecs =
        subResources.stream()
            .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
            .collect(Collectors.toSet());

    return resourceSpecs.entrySet().stream()
        .allMatch(
            entry ->
                isAPIAuthorized(
                    session,
                    lookupAPIPrivilege(ENTITY, apiOperation, entry.getKey()),
                    entry.getValue(),
                    subResourceSpecs));
  }

  /** Authorizes a request when REST API authorization is enabled. */
  public static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final DisjunctivePrivilegeGroup privilegeGroup,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources) {
    if (AuthUtil.isRestApiAuthorizationEnabled) {
      return isAuthorized(session, privilegeGroup, resourceSpec, subResources);
    }
    return true;
  }

  /** Returns the tag-specific privilege required to modify tags on an entity. */
  public static PoliciesConfig.Privilege tagModificationPrivilege(
      @Nonnull final Urn entityUrn, final boolean fieldLevelTags) {
    if (fieldLevelTags && DATASET_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      return PoliciesConfig.EDIT_DATASET_COL_TAGS_PRIVILEGE;
    }
    return PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE;
  }

  /**
   * Entity privileges for {@code operation}, plus the specific tag privilege on an update or
   * delete. A create uses only the entity create group, so a tag privilege cannot create the
   * entity. A schema field matches a policy on its parent dataset or on the schema field itself. An
   * empty tag set writes nothing.
   */
  public static DisjunctivePrivilegeGroup tagModificationPrivilegeGroup(
      @Nonnull final Urn entityUrn,
      @Nonnull final ApiOperation operation,
      @Nonnull final PoliciesConfig.Privilege specificTagPrivilege) {
    final ApiOperation effective = operationFor(entityUrn, operation);
    final DisjunctivePrivilegeGroup entityPrivileges =
        entityUpdatePrivilegeGroup(authorizationTarget(entityUrn), effective);
    if (CREATE.equals(effective)) {
      return entityPrivileges;
    }
    return withPrivilege(entityPrivileges, specificTagPrivilege);
  }

  public static boolean isAuthorizedForTagModification(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Urn entityUrn,
      @Nonnull final Collection<Urn> tagUrns,
      @Nonnull final PoliciesConfig.Privilege specificTagPrivilege) {
    return isAuthorizedForSubResources(
        session,
        entityUrn,
        tagUrns,
        tagModificationPrivilegeGroup(entityUrn, UPDATE, specificTagPrivilege));
  }

  /**
   * Whether a policy grants changing {@code tagUrns}. Denied when no policy grants. An empty set
   * writes nothing and is allowed here; the caller still has its own coarse check.
   */
  public static boolean isAPIAuthorizedForTagModification(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Urn entityUrn,
      @Nonnull final Collection<Urn> tagUrns,
      @Nonnull final PoliciesConfig.Privilege specificTagPrivilege,
      @Nonnull final ApiOperation operation) {
    return isAPIAuthorizedForSubResources(
        session,
        entityUrn,
        tagUrns,
        tagModificationPrivilegeGroup(entityUrn, operation, specificTagPrivilege));
  }

  /**
   * Privileges that cover a structured-property value write. A schema field matches a policy on its
   * parent dataset or on the schema field itself, using Edit Entity or Edit Dataset Column
   * Properties. Other entities use that entity's privileges for {@code operation}. A create does
   * not add Edit Entity Properties, so that privilege cannot create the entity. An update or delete
   * does.
   */
  public static DisjunctivePrivilegeGroup structuredPropertyModificationPrivilegeGroup(
      @Nonnull final Urn entityUrn, @Nonnull final ApiOperation operation) {
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      return new DisjunctivePrivilegeGroup(
          List.of(
              new ConjunctivePrivilegeGroup(
                  List.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType())),
              new ConjunctivePrivilegeGroup(
                  List.of(PoliciesConfig.EDIT_DATASET_COL_PROPERTIES_PRIVILEGE.getType()))));
    }
    final DisjunctivePrivilegeGroup entityPrivileges =
        entityUpdatePrivilegeGroup(entityUrn, operation);
    if (CREATE.equals(operation)) {
      return entityPrivileges;
    }
    return withPrivilege(entityPrivileges, PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE);
  }

  /**
   * Whether a policy grants changing {@code propertyUrns}. Denied when no policy grants. An empty
   * set writes nothing and is allowed here.
   */
  public static boolean isAPIAuthorizedForStructuredPropertyModification(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Urn entityUrn,
      @Nonnull final Collection<Urn> propertyUrns,
      @Nonnull final ApiOperation operation) {
    return isAPIAuthorizedForSubResources(
        session,
        entityUrn,
        propertyUrns,
        structuredPropertyModificationPrivilegeGroup(entityUrn, operation));
  }

  /** GraphQL counterpart of {@link #isAPIAuthorizedForStructuredPropertyModification}. */
  public static boolean isAuthorizedForStructuredPropertyModification(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Urn entityUrn,
      @Nonnull final Collection<Urn> propertyUrns,
      @Nonnull final ApiOperation operation) {
    return isAuthorizedForSubResources(
        session,
        entityUrn,
        propertyUrns,
        structuredPropertyModificationPrivilegeGroup(entityUrn, operation));
  }

  /**
   * Parent dataset for a schema field; the entity itself otherwise. Privilege lookup uses this
   * dataset. A schema-field write is also matched against the schema field URN.
   */
  @Nonnull
  public static Urn authorizationTarget(@Nonnull final Urn entityUrn) {
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityUrn.getEntityType())
        && !entityUrn.getEntityKey().getParts().isEmpty()) {
      return UrnUtils.getUrn(entityUrn.getEntityKey().get(0));
    }
    return entityUrn;
  }

  /**
   * Resources a schema-field tag or structured-property write is matched against. The parent
   * dataset is first, then the schema field, so a policy on either can grant. Other entities are
   * matched as themselves.
   */
  @Nonnull
  private static List<Urn> authorizationTargets(@Nonnull final Urn entityUrn) {
    Urn parent = authorizationTarget(entityUrn);
    if (parent.equals(entityUrn)) {
      return List.of(entityUrn);
    }
    return List.of(parent, entityUrn);
  }

  @Nonnull
  private static EntitySpec entitySpec(@Nonnull final Urn urn) {
    return new EntitySpec(urn.getEntityType(), urn.toString());
  }

  /** Sub-resource that matches any privilegeConstraints list. Used by coarse checks. */
  @Nonnull
  public static EntitySpec wildcardSubResource() {
    return new EntitySpec(WILDCARD_URN.getEntityType(), WILDCARD_URN.toString());
  }

  /**
   * Privilege path for an entity write given change type and existence. A missing entity's upsert,
   * update, restate, or patch is a create. An existing entity's is an update. Aspect-level {@link
   * ChangeType#CREATE} is always an update. Shared by domain writes and property and tag writes.
   */
  @Nonnull
  public static ApiOperation resolveEntityWriteOperation(
      @Nonnull final ChangeType changeType, final boolean entityExists) {
    switch (changeType) {
      case CREATE_ENTITY:
        return entityExists ? UPDATE : CREATE;
      case CREATE:
        return UPDATE;
      case PATCH:
        return entityExists ? UPDATE : CREATE;
      case UPSERT:
      case UPDATE:
      case RESTATE:
        return entityExists ? UPDATE : CREATE;
      case DELETE:
        return DELETE;
      default:
        return UPDATE;
    }
  }

  /**
   * The operation a tag or structured-property write is authorized as. A schema field is always an
   * update of its parent dataset. Every other entity uses {@link #resolveEntityWriteOperation}.
   */
  @Nonnull
  public static ApiOperation subResourceWriteOperation(
      @Nonnull final ChangeType changeType, @Nonnull final Urn entityUrn, final boolean exists) {
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      return UPDATE;
    }
    return resolveEntityWriteOperation(changeType, exists);
  }

  /**
   * Whether {@link #subResourceWriteOperation} depends on the entity existing. A schema field, a
   * delete, and an aspect-level create do not.
   */
  public static boolean subResourceWriteOperationNeedsExistence(
      @Nonnull final ChangeType changeType, @Nonnull final Urn entityUrn) {
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      return false;
    }
    switch (changeType) {
      case UPSERT:
      case UPDATE:
      case RESTATE:
      case PATCH:
      case CREATE_ENTITY:
        return true;
      default:
        return false;
    }
  }

  /**
   * Coarse check the ingest gate makes for an aspect that holds only the sub-resource, {@code
   * structuredProperties} or {@code globalTags}: the shared privilege set against a wildcard
   * sub-resource. A schema field matches a policy on the parent dataset or on the schema field. It
   * may deny earlier than the validator but never denies a write the validator would allow.
   *
   * <p>Not for {@code schemaMetadata} or {@code editableSchemaMetadata}. Those aspects carry field
   * types, descriptions and terms alongside tags, so a tag privilege must not admit a write to
   * them. They keep the entity privilege check and the validator then limits the tag changes.
   *
   * @param exists whether the entity exists; ignored for a schema field, which is always an update
   */
  public static boolean isCoarseSubResourceWriteAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ChangeType changeType,
      @Nonnull final Urn entityUrn,
      @Nonnull final String aspectName,
      final boolean exists) {
    final ApiOperation operation = subResourceWriteOperation(changeType, entityUrn, exists);
    final DisjunctivePrivilegeGroup group;
    if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName)) {
      group = structuredPropertyModificationPrivilegeGroup(entityUrn, operation);
    } else {
      group =
          tagModificationPrivilegeGroup(
              entityUrn, operation, PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE);
    }
    Set<EntitySpec> subResources = Set.of(wildcardSubResource());
    for (Urn target : authorizationTargets(entityUrn)) {
      if (isAPIAuthorized(session, group, entitySpec(target), subResources)) {
        return true;
      }
    }
    return false;
  }

  private static ApiOperation operationFor(
      @Nonnull final Urn entityUrn, @Nonnull final ApiOperation operation) {
    return SCHEMA_FIELD_ENTITY_NAME.equals(entityUrn.getEntityType()) ? UPDATE : operation;
  }

  private static DisjunctivePrivilegeGroup entityUpdatePrivilegeGroup(
      @Nonnull final Urn entityUrn, @Nonnull final ApiOperation operation) {
    return buildDisjunctivePrivilegeGroup(
        lookupAPIPrivilege(ENTITY, operation, entityUrn.getEntityType()));
  }

  private static DisjunctivePrivilegeGroup withPrivilege(
      @Nonnull final DisjunctivePrivilegeGroup base,
      @Nonnull final PoliciesConfig.Privilege privilege) {
    List<ConjunctivePrivilegeGroup> groups =
        new java.util.ArrayList<>(base.getAuthorizedPrivilegeGroups());
    groups.add(new ConjunctivePrivilegeGroup(List.of(privilege.getType())));
    return new DisjunctivePrivilegeGroup(groups);
  }

  private static boolean isAPIAuthorizedForSubResources(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Urn entityUrn,
      @Nonnull final Collection<Urn> subResourceUrns,
      @Nonnull final DisjunctivePrivilegeGroup privilegeGroup) {
    if (subResourceUrns.isEmpty()) {
      return true;
    }
    Set<EntitySpec> subResources = subResourceSpecs(subResourceUrns);
    for (Urn target : authorizationTargets(entityUrn)) {
      if (isAPIAuthorized(session, privilegeGroup, entitySpec(target), subResources)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isAuthorizedForSubResources(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Urn entityUrn,
      @Nonnull final Collection<Urn> subResourceUrns,
      @Nonnull final DisjunctivePrivilegeGroup privilegeGroup) {
    if (subResourceUrns.isEmpty()) {
      return true;
    }
    Set<EntitySpec> subResources = subResourceSpecs(subResourceUrns);
    for (Urn target : authorizationTargets(entityUrn)) {
      if (isAuthorized(session, privilegeGroup, entitySpec(target), subResources)) {
        return true;
      }
    }
    return false;
  }

  private static Set<EntitySpec> subResourceSpecs(@Nonnull final Collection<Urn> subResourceUrns) {
    return subResourceUrns.stream()
        .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
        .collect(Collectors.toSet());
  }

  public static boolean isAPIAuthorizedEntityType(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final String entityType) {
    return isAPIAuthorizedEntityType(session, ENTITY, apiOperation, List.of(entityType));
  }

  public static boolean isAPIAuthorizedEntityType(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final String entityType) {
    return isAPIAuthorizedEntityType(session, apiGroup, apiOperation, List.of(entityType));
  }

  public static boolean isAPIAuthorizedEntityType(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<String> entityTypes) {

    return isAPIAuthorizedEntityType(session, ENTITY, apiOperation, entityTypes);
  }

  public static boolean isAPIAuthorizedEntityType(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<String> entityTypes) {

    return entityTypes.stream()
        .distinct()
        .allMatch(
            entityType ->
                isAPIAuthorized(
                    session,
                    lookupAPIPrivilege(apiGroup, apiOperation, entityType),
                    new EntitySpec(entityType, ""),
                    Collections.emptyList()));
  }

  public static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation) {
    return isAPIAuthorized(
        session,
        lookupAPIPrivilege(apiGroup, apiOperation, null),
        (EntitySpec) null,
        Collections.emptyList());
  }

  public static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege,
      @Nullable final EntitySpec resource) {
    return isAPIAuthorized(
        session, Disjunctive.disjoint(privilege), resource, Collections.emptyList());
  }

  public static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege) {
    return isAPIAuthorized(
        session, Disjunctive.disjoint(privilege), (EntitySpec) null, Collections.emptyList());
  }

  /**
   * Allow specific privilege OR MANAGE_SYSTEM_OPERATIONS_PRIVILEGE
   *
   * @param session authorization session
   * @param privilege specific privilege
   * @return authorized status
   */
  public static boolean isAPIOperationsAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege) {
    return isAPIAuthorized(
        session,
        Disjunctive.disjoint(privilege, MANAGE_SYSTEM_OPERATIONS_PRIVILEGE),
        (EntitySpec) null,
        Collections.emptyList());
  }

  public static boolean isAPIOperationsAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege,
      @Nullable final EntitySpec resource) {
    return isAPIAuthorized(
        session,
        Disjunctive.disjoint(privilege, MANAGE_SYSTEM_OPERATIONS_PRIVILEGE),
        resource,
        Collections.emptyList());
  }

  public static boolean isAPIOperationsAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege,
      @Nullable final EntitySpec resource,
      @Nonnull final Collection<EntitySpec> subResources) {
    return isAPIAuthorized(
        session,
        Disjunctive.disjoint(privilege, MANAGE_SYSTEM_OPERATIONS_PRIVILEGE),
        resource,
        subResources);
  }

  private static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nullable final EntitySpec resource,
      @Nonnull final Collection<EntitySpec> subResources) {
    return isAPIAuthorized(
        session, privileges, resource != null ? List.of(resource) : List.of(), subResources);
  }

  private static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull final Collection<EntitySpec> resources,
      @Nonnull final Collection<EntitySpec> subResources) {
    if (AuthUtil.isRestApiAuthorizationEnabled) {
      return isAuthorized(
          session, buildDisjunctivePrivilegeGroup(privileges), resources, subResources);
    } else {
      return true;
    }
  }

  /** GraphQL Methods */
  public static boolean canViewEntity(
      @Nonnull final AuthorizationSession session, @Nonnull Urn urn) {
    return canViewEntity(session, List.of(urn));
  }

  public static boolean canViewEntity(
      @Nonnull final AuthorizationSession session, @Nonnull final Collection<Urn> urns) {

    return isAuthorizedEntityUrns(session, READ, urns);
  }

  public static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation) {
    return isAuthorized(session, lookupAPIPrivilege(apiGroup, apiOperation, null), null);
  }

  public static boolean isAuthorizedEntityType(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<String> entityTypes) {

    return entityTypes.stream()
        .distinct()
        .allMatch(
            entityType ->
                isAuthorized(
                    session,
                    lookupEntityAPIPrivilege(apiOperation, entityType),
                    new EntitySpec(entityType, "")));
  }

  public static boolean isAuthorizedEntityUrns(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<Urn> urns) {
    return isAuthorizedUrns(session, ENTITY, apiOperation, urns);
  }

  public static boolean isAuthorizedUrns(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation,
      @Nonnull final Collection<Urn> urns) {

    Map<String, List<EntitySpec>> resourceSpecs =
        urns.stream()
            .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
            .collect(Collectors.groupingBy(EntitySpec::getType));

    return resourceSpecs.entrySet().stream()
        .allMatch(
            entry -> {
              Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges =
                  lookupAPIPrivilege(apiGroup, apiOperation, entry.getKey());
              return entry.getValue().stream()
                  .allMatch(entitySpec -> isAuthorized(session, privileges, entitySpec));
            });
  }

  public static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege) {
    return isAuthorized(
        session,
        buildDisjunctivePrivilegeGroup(Disjunctive.disjoint(privilege)),
        (EntitySpec) null);
  }

  public static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege,
      @Nullable final EntitySpec entitySpec) {
    return isAuthorized(
        session, buildDisjunctivePrivilegeGroup(Disjunctive.disjoint(privilege)), entitySpec);
  }

  private static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nullable EntitySpec maybeResourceSpec) {
    return isAuthorized(session, buildDisjunctivePrivilegeGroup(privileges), maybeResourceSpec);
  }

  public static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final DisjunctivePrivilegeGroup privilegeGroup,
      @Nullable final EntitySpec resourceSpec) {
    return isAuthorized(session, privilegeGroup, resourceSpec, Collections.emptyList());
  }

  public static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final DisjunctivePrivilegeGroup privilegeGroup,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources) {

    for (ConjunctivePrivilegeGroup conjunctive : privilegeGroup.getAuthorizedPrivilegeGroups()) {
      if (isAuthorized(session, conjunctive, resourceSpec, subResources)) {
        return true;
      }
    }

    return false;
  }

  private static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ConjunctivePrivilegeGroup requiredPrivileges,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources) {

    // if no privileges are required, deny
    if (requiredPrivileges.getRequiredPrivileges().isEmpty()) {
      return false;
    }

    // Each privilege in a group _must_ all be true to permit the operation.
    for (final String privilege : requiredPrivileges.getRequiredPrivileges()) {
      // Create and evaluate an Authorization request.
      if (isDenied(session, privilege, resourceSpec, subResources)) {
        // Short circuit.
        return false;
      }
    }
    return true;
  }

  private static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final DisjunctivePrivilegeGroup privilegeGroup,
      @Nonnull final Collection<EntitySpec> resourceSpecs,
      @Nonnull final Collection<EntitySpec> subResources) {

    if (resourceSpecs.isEmpty()) {
      return isAuthorized(session, privilegeGroup, (EntitySpec) null);
    }

    return resourceSpecs.stream()
        .allMatch(spec -> isAuthorized(session, privilegeGroup, spec, subResources));
  }

  /** Common Methods */

  /**
   * Based on an API group and operation return privileges. Broad level privileges that are not
   * specific to an Entity/Aspect.
   *
   * @param apiGroup
   * @param apiOperation
   * @return
   */
  public static Disjunctive<Conjunctive<PoliciesConfig.Privilege>> lookupAPIPrivilege(
      @Nonnull ApiGroup apiGroup, @Nonnull ApiOperation apiOperation, @Nullable String entityType) {

    if (ApiGroup.ENTITY.equals(apiGroup) && entityType != null) {
      return lookupEntityAPIPrivilege(apiOperation, Set.of(entityType)).get(entityType);
    }

    Map<ApiOperation, Disjunctive<Conjunctive<PoliciesConfig.Privilege>>> privMap =
        API_PRIVILEGE_MAP.getOrDefault(apiGroup, Map.of());

    switch (apiOperation) {
      // Manage is a conjunction of UPDATE and DELETE
      case MANAGE:
        return Disjunctive.conjoin(
            privMap.getOrDefault(ApiOperation.UPDATE, DENY_ACCESS),
            privMap.getOrDefault(ApiOperation.DELETE, DENY_ACCESS));
      default:
        return privMap.getOrDefault(apiOperation, DENY_ACCESS);
    }
  }

  /**
   * Returns map of entityType to privileges required for that entity
   *
   * @param apiOperation
   * @param entityTypes
   * @return
   */
  @VisibleForTesting
  static Map<String, Disjunctive<Conjunctive<PoliciesConfig.Privilege>>> lookupEntityAPIPrivilege(
      @Nonnull ApiOperation apiOperation, @Nonnull Collection<String> entityTypes) {

    return entityTypes.stream()
        .distinct()
        .map(
            entityType -> {

              // Check entity specific privilege map, otherwise default to generic entity
              Map<ApiOperation, Disjunctive<Conjunctive<PoliciesConfig.Privilege>>> privMap =
                  API_ENTITY_PRIVILEGE_MAP.getOrDefault(
                      entityType, API_PRIVILEGE_MAP.getOrDefault(ApiGroup.ENTITY, Map.of()));

              switch (apiOperation) {
                // Manage is a conjunction of UPDATE and DELETE
                case MANAGE:
                  return Pair.of(
                      entityType,
                      Disjunctive.conjoin(
                          privMap.getOrDefault(ApiOperation.UPDATE, DENY_ACCESS),
                          privMap.getOrDefault(ApiOperation.DELETE, DENY_ACCESS)));
                default:
                  // otherwise default to generic entity
                  return Pair.of(entityType, privMap.getOrDefault(apiOperation, DENY_ACCESS));
              }
            })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  @VisibleForTesting
  static Disjunctive<Conjunctive<PoliciesConfig.Privilege>> lookupEntityAPIPrivilege(
      @Nonnull ApiOperation apiOperation, @Nonnull String entityType) {
    return lookupEntityAPIPrivilege(apiOperation, Set.of(entityType)).get(entityType);
  }

  public static DisjunctivePrivilegeGroup buildDisjunctivePrivilegeGroup(
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation,
      @Nullable final String entityType) {
    return buildDisjunctivePrivilegeGroup(lookupAPIPrivilege(apiGroup, apiOperation, entityType));
  }

  public static DisjunctivePrivilegeGroup buildDisjunctivePrivilegeGroup(
      final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges) {
    return new DisjunctivePrivilegeGroup(
        privileges.stream()
            .map(
                priv ->
                    new ConjunctivePrivilegeGroup(
                        priv.stream()
                            .map(PoliciesConfig.Privilege::getType)
                            .collect(Collectors.toList())))
            .collect(Collectors.toList()));
  }

  private static boolean isDenied(
      @Nonnull final AuthorizationSession session,
      @Nonnull final String privilege,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources) {
    // Create and evaluate an Authorization request.
    final AuthorizationResult result = session.authorize(privilege, resourceSpec, subResources);
    return AuthorizationResult.Type.DENY.equals(result.getType());
  }

  protected AuthUtil() {}
}
