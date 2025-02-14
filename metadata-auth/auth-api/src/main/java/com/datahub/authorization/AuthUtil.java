package com.datahub.authorization;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_FLOW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_FEATURE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_FEATURE_TABLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_PRIMARY_KEY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.NOTEBOOK_ENTITY_NAME;
import static com.linkedin.metadata.Constants.REST_API_AUTHORIZATION_ENABLED_ENV;
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
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.HttpStatus;

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
public class AuthUtil {

  /**
   * This should generally follow the policy creation UI with a few exceptions for users, groups,
   * containers, etc so that the platform still functions as expected.
   */
  public static final Set<String> VIEW_RESTRICTED_ENTITY_TYPES =
      ImmutableSet.of(
          DATASET_ENTITY_NAME,
          DASHBOARD_ENTITY_NAME,
          CHART_ENTITY_NAME,
          ML_MODEL_ENTITY_NAME,
          ML_FEATURE_ENTITY_NAME,
          ML_MODEL_GROUP_ENTITY_NAME,
          ML_FEATURE_TABLE_ENTITY_NAME,
          ML_PRIMARY_KEY_ENTITY_NAME,
          DATA_FLOW_ENTITY_NAME,
          DATA_JOB_ENTITY_NAME,
          GLOSSARY_TERM_ENTITY_NAME,
          GLOSSARY_NODE_ENTITY_NAME,
          DOMAIN_ENTITY_NAME,
          DATA_PRODUCT_ENTITY_NAME,
          NOTEBOOK_ENTITY_NAME);

  /** OpenAPI/Rest.li Methods */
  public static List<Pair<MetadataChangeProposal, Integer>> isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<MetadataChangeProposal> mcps) {

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
            changeUrnMCPs.stream().map(Pair::getFirst).collect(Collectors.toSet()));

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

    return changeTypeUrns.stream()
        .distinct()
        .map(
            changeTypePair -> {
              final Urn urn = changeTypePair.getSecond();
              switch (changeTypePair.getFirst()) {
                case CREATE:
                case UPSERT:
                case UPDATE:
                case RESTATE:
                case PATCH:
                  if (!isAPIAuthorized(
                      session,
                      lookupAPIPrivilege(apiGroup, UPDATE, urn.getEntityType()),
                      new EntitySpec(urn.getEntityType(), urn.toString()))) {
                    return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                  }
                  break;
                case CREATE_ENTITY:
                  if (!isAPIAuthorized(
                      session,
                      lookupAPIPrivilege(apiGroup, CREATE, urn.getEntityType()),
                      new EntitySpec(urn.getEntityType(), urn.toString()))) {
                    return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                  }
                  break;
                case DELETE:
                  if (!isAPIAuthorized(
                      session,
                      lookupAPIPrivilege(apiGroup, DELETE, urn.getEntityType()),
                      new EntitySpec(urn.getEntityType(), urn.toString()))) {
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
        session, lookupAPIPrivilege(apiGroup, apiOperation, null), resourceSpecs);
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
                    entry.getValue()));
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
                    new EntitySpec(entityType, "")));
  }

  public static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ApiGroup apiGroup,
      @Nonnull final ApiOperation apiOperation) {
    return isAPIAuthorized(
        session, lookupAPIPrivilege(apiGroup, apiOperation, null), (EntitySpec) null);
  }

  public static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege,
      @Nullable final EntitySpec resource) {
    return isAPIAuthorized(session, Disjunctive.disjoint(privilege), resource);
  }

  public static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege) {
    return isAPIAuthorized(session, Disjunctive.disjoint(privilege), (EntitySpec) null);
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
        (EntitySpec) null);
  }

  public static boolean isAPIOperationsAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final PoliciesConfig.Privilege privilege,
      @Nullable final EntitySpec resource) {
    return isAPIAuthorized(
        session, Disjunctive.disjoint(privilege, MANAGE_SYSTEM_OPERATIONS_PRIVILEGE), resource);
  }

  private static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nullable final EntitySpec resource) {
    return isAPIAuthorized(session, privileges, resource != null ? List.of(resource) : List.of());
  }

  private static boolean isAPIAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull final Collection<EntitySpec> resources) {
    if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))) {
      return isAuthorized(session, buildDisjunctivePrivilegeGroup(privileges), resources);
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

    for (ConjunctivePrivilegeGroup conjunctive : privilegeGroup.getAuthorizedPrivilegeGroups()) {
      if (isAuthorized(session, conjunctive, resourceSpec)) {
        return true;
      }
    }

    return false;
  }

  private static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final ConjunctivePrivilegeGroup requiredPrivileges,
      @Nullable final EntitySpec resourceSpec) {

    // if no privileges are required, deny
    if (requiredPrivileges.getRequiredPrivileges().isEmpty()) {
      return false;
    }

    // Each privilege in a group _must_ all be true to permit the operation.
    for (final String privilege : requiredPrivileges.getRequiredPrivileges()) {
      // Create and evaluate an Authorization request.
      if (isDenied(session, privilege, resourceSpec)) {
        // Short circuit.
        return false;
      }
    }
    return true;
  }

  private static boolean isAuthorized(
      @Nonnull final AuthorizationSession session,
      @Nonnull final DisjunctivePrivilegeGroup privilegeGroup,
      @Nonnull final Collection<EntitySpec> resourceSpecs) {

    if (resourceSpecs.isEmpty()) {
      return isAuthorized(session, privilegeGroup, (EntitySpec) null);
    }

    return resourceSpecs.stream().allMatch(spec -> isAuthorized(session, privilegeGroup, spec));
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
      @Nullable final EntitySpec resourceSpec) {
    // Create and evaluate an Authorization request.
    final AuthorizationResult result = session.authorize(privilege, resourceSpec);
    return AuthorizationResult.Type.DENY.equals(result.getType());
  }

  private AuthUtil() {}
}
