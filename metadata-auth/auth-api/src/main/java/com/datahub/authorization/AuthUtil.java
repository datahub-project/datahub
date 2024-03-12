package com.datahub.authorization;

import static com.linkedin.metadata.Constants.REST_API_AUTHORIZATION_ENABLED_ENV;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
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
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.HttpStatus;

public class AuthUtil {

  public static List<Pair<MetadataChangeProposal, Integer>> isAPIAuthorized(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull ApiGroup apiGroup,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Collection<MetadataChangeProposal> mcps) {

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
            authentication,
            authorizer,
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
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull ApiGroup apiGroup,
      @Nonnull Collection<Pair<ChangeType, Urn>> changeTypeUrns) {

    return changeTypeUrns.stream()
        .distinct()
        .map(
            changeTypePair -> {
              final Urn urn = changeTypePair.getSecond();
              switch (changeTypePair.getFirst()) {
                case UPSERT:
                case UPDATE:
                case RESTATE:
                case PATCH:
                  if (!isAPIAuthorized(
                      authentication,
                      authorizer,
                      PoliciesConfig.lookupAPIPrivilege(apiGroup, UPDATE),
                      new EntitySpec(urn.getEntityType(), urn.toString()))) {
                    return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                  }
                  break;
                case CREATE:
                  if (!isAPIAuthorized(
                      authentication,
                      authorizer,
                      PoliciesConfig.lookupAPIPrivilege(apiGroup, CREATE),
                      new EntitySpec(urn.getEntityType(), urn.toString()))) {
                    return Pair.of(changeTypePair, HttpStatus.SC_FORBIDDEN);
                  }
                  break;
                case DELETE:
                  if (!isAPIAuthorized(
                      authentication,
                      authorizer,
                      PoliciesConfig.lookupAPIPrivilege(apiGroup, DELETE),
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
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull SearchResult result) {
    return isAPIAuthorizedUrns(
        authentication,
        authorizer,
        privileges,
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull ScrollResult result) {
    return isAPIAuthorizedUrns(
        authentication,
        authorizer,
        privileges,
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull LineageSearchResult result) {
    return isAPIAuthorizedUrns(
        authentication,
        authorizer,
        privileges,
        result.getEntities().stream()
            .map(LineageSearchEntity::getEntity)
            .collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull LineageScrollResult result) {
    return isAPIAuthorizedUrns(
        authentication,
        authorizer,
        privileges,
        result.getEntities().stream()
            .map(LineageSearchEntity::getEntity)
            .collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull AutoCompleteResult result) {
    return isAPIAuthorizedUrns(
        authentication,
        authorizer,
        privileges,
        result.getEntities().stream().map(AutoCompleteEntity::getUrn).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull BrowseResult result) {
    return isAPIAuthorizedUrns(
        authentication,
        authorizer,
        privileges,
        result.getEntities().stream().map(BrowseResultEntity::getUrn).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedUrns(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull Collection<Urn> urns) {
    List<EntitySpec> resourceSpecs =
        urns.stream()
            .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
            .collect(Collectors.toList());

    return isAPIAuthorized(authentication, authorizer, privileges, resourceSpecs);
  }

  public static boolean isAPIAuthorized(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges) {
    return isAPIAuthorized(authentication, authorizer, privileges, List.of());
  }

  public static boolean isAPIAuthorizedEntityType(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull final String entityType) {
    return isAPIAuthorizedEntityType(authentication, authorizer, privileges, List.of(entityType));
  }

  public static boolean isAPIAuthorizedEntityType(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull final Collection<String> entityTypes) {
    return isAPIAuthorized(
        authentication,
        authorizer,
        privileges,
        entityTypes.stream()
            .map(entityType -> new EntitySpec(entityType, ""))
            .collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorized(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nullable final EntitySpec resource) {
    return isAPIAuthorized(
        authentication, authorizer, privileges, resource != null ? List.of(resource) : List.of());
  }

  public static boolean isAPIAuthorized(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nonnull final Collection<EntitySpec> resources) {
    if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))) {
      return isAuthorized(
          authorizer,
          authentication.getActor().toUrnStr(),
          buildDisjunctivePrivilegeGroup(privileges),
          resources);
    } else {
      return true;
    }
  }

  public static boolean canViewEntity(
      @Nonnull Authentication authentication, @Nonnull Authorizer authorizer, @Nonnull Urn urn) {
    return canViewEntity(authentication, authorizer, List.of(urn));
  }

  public static boolean canViewEntity(
      @Nonnull Authentication authentication,
      @Nonnull Authorizer authorizer,
      @Nonnull Collection<Urn> urns) {

    final List<EntitySpec> resourceSpecs =
        urns.stream()
            .map(urn -> new EntitySpec(urn.getEntityType(), urn.toString()))
            .collect(Collectors.toList());

    final DisjunctivePrivilegeGroup orGroup = buildAPIDisjunctivePrivilegeGroup(ENTITY, READ);

    final String actor = authentication.getActor().toUrnStr();
    return isAuthorized(authorizer, actor, orGroup, resourceSpecs);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull final PoliciesConfig.Privilege privilege) {
    return isAuthorized(authorizer, actor, Disjunctive.disjoint(privilege));
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges,
      @Nullable EntitySpec maybeResourceSpec) {
    return isAuthorized(
        authorizer, actor, buildDisjunctivePrivilegeGroup(privileges), maybeResourceSpec);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup,
      @Nullable EntitySpec resourceSpec) {

    for (ConjunctivePrivilegeGroup conjunctive : privilegeGroup.getAuthorizedPrivilegeGroups()) {
      if (isAuthorized(authorizer, actor, conjunctive, resourceSpec)) {
        return true;
      }
    }

    return false;
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull ConjunctivePrivilegeGroup requiredPrivileges,
      @Nullable EntitySpec resourceSpec) {

    // if no privileges are required, deny
    if (requiredPrivileges.getRequiredPrivileges().isEmpty()) {
      return false;
    }

    // Each privilege in a group _must_ all be true to permit the operation.
    for (final String privilege : requiredPrivileges.getRequiredPrivileges()) {
      // Create and evaluate an Authorization request.
      if (isDenied(authorizer, actor, privilege, resourceSpec)) {
        // Short circuit.
        return false;
      }
    }
    return true;
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges) {
    return isAuthorized(authorizer, actor, privileges, null);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup,
      @Nonnull Collection<EntitySpec> resourceSpecs) {

    if (resourceSpecs.isEmpty()) {
      return isAuthorized(authorizer, actor, privilegeGroup, (EntitySpec) null);
    }

    return resourceSpecs.stream()
        .allMatch(spec -> isAuthorized(authorizer, actor, privilegeGroup, spec));
  }

  public static DisjunctivePrivilegeGroup buildAPIDisjunctivePrivilegeGroup(
      ApiGroup apiGroup, ApiOperation apiOperation) {
    return buildDisjunctivePrivilegeGroup(
        PoliciesConfig.lookupAPIPrivilege(apiGroup, apiOperation));
  }

  public static DisjunctivePrivilegeGroup buildDisjunctivePrivilegeGroup(
      Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges) {
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
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull String privilege,
      @Nullable EntitySpec resourceSpec) {
    // Create and evaluate an Authorization request.
    final AuthorizationRequest request =
        new AuthorizationRequest(actor, privilege, Optional.ofNullable(resourceSpec));
    final AuthorizationResult result = authorizer.authorize(request);
    return AuthorizationResult.Type.DENY.equals(result.getType());
  }

  private AuthUtil() {}
}
