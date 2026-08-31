package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.QUERY_SUBJECTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainAssociation;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjects;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Batch authorization helpers for cross-entity aspect writes and Query entity reads. */
public final class EntityAspectAuthorizationUtils {

  private static final ConjunctivePrivilegeGroup ALL_ENTITY_PRIVILEGES =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private static final DisjunctivePrivilegeGroup MANAGE_DATA_PRODUCTS_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              ALL_ENTITY_PRIVILEGES,
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.MANAGE_DATA_PRODUCTS_PRIVILEGE.getType()))));

  private static final DisjunctivePrivilegeGroup EDIT_ENTITY_QUERIES_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              ALL_ENTITY_PRIVILEGES,
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType()))));

  // Viewing a query requires the explicit view privilege (or the ability to edit queries,
  // which implies viewing them).
  private static final DisjunctivePrivilegeGroup VIEW_ENTITY_QUERIES_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              ALL_ENTITY_PRIVILEGES,
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE.getType())),
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType()))));

  private static final DisjunctivePrivilegeGroup EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              ALL_ENTITY_PRIVILEGES,
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGE.getType()))));

  private EntityAspectAuthorizationUtils() {}

  /**
   * Authorization candidates for a {@code logicalParent} write on {@code urn}, ordered for
   * evaluation: containing dataset first for schema fields (most common), then the entity URN.
   */
  @Nonnull
  public static LinkedHashSet<Urn> resolveLogicalParentAuthorizationCandidates(@Nonnull Urn urn) {
    LinkedHashSet<Urn> candidates = new LinkedHashSet<>();
    if (SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())) {
      candidates.add(UrnUtils.getUrn(urn.getEntityKey().get(0)));
      candidates.add(urn);
    } else {
      candidates.add(urn);
    }
    return candidates;
  }

  /**
   * Returns true when the actor has {@code EDIT_ENTITY} on {@code urn} for a logical-parent write.
   * Datasets are checked directly. Schema fields succeed when either the containing dataset or the
   * schema field URN is authorized.
   */
  public static boolean isAuthorizedToEditLogicalParentEntity(
      @Nonnull AuthorizationSession session, @Nonnull Urn urn) {
    for (Urn candidate : resolveLogicalParentAuthorizationCandidates(urn)) {
      if (com.datahub.authorization.AuthUtil.isAuthorizedEntityUrns(
          session, ApiOperation.UPDATE, Set.of(candidate))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true when the actor may view {@code schemaFieldUrn}. Schema fields inherit VIEW from
   * the containing parent URN encoded in the schemaField key (typically a dataset), then fall back
   * to a direct grant on the schemaField URN itself.
   *
   * <p>Uses {@link #resolveLogicalParentAuthorizationCandidates(Urn)} so write and view paths share
   * the same parent resolution. Non-schemaField URNs are checked directly via {@link
   * com.datahub.authorization.AuthUtil#canViewEntity}.
   */
  public static boolean canViewSchemaFieldEntity(
      @Nonnull AuthorizationSession session, @Nonnull Urn schemaFieldUrn) {
    if (!SCHEMA_FIELD_ENTITY_NAME.equals(schemaFieldUrn.getEntityType())) {
      return com.datahub.authorization.AuthUtil.canViewEntity(session, schemaFieldUrn);
    }
    for (Urn candidate : resolveLogicalParentAuthorizationCandidates(schemaFieldUrn)) {
      if (com.datahub.authorization.AuthUtil.canViewEntity(session, candidate)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isSchemaFieldEntity(@Nonnull Urn urn) {
    return SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType());
  }

  /**
   * Returns true when the actor may write {@code logicalParent} on {@code childUrn}. Setting a
   * parent requires {@code EDIT_ENTITY} on both the child and proposed parent — each side is
   * evaluated independently (dataset or schema field URN for that side). Clearing a parent requires
   * authorization on the child side only.
   */
  public static boolean isAuthorizedToEditLogicalParent(
      @Nonnull AuthorizationSession session,
      @Nonnull Urn childUrn,
      @Nullable Urn proposedParentUrn) {
    if (proposedParentUrn == null) {
      return isAuthorizedToEditLogicalParentEntity(session, childUrn);
    }
    return isAuthorizedToEditLogicalParentEntity(session, childUrn)
        && isAuthorizedToEditLogicalParentEntity(session, proposedParentUrn);
  }

  /**
   * Returns child URNs whose {@code logicalParent} write is unauthorized. Delegates to {@link
   * #isAuthorizedToEditLogicalParent(AuthorizationSession, Urn, Urn)} per entry; map values are the
   * child URN plus an optional proposed parent URN.
   */
  @Nonnull
  public static Set<Urn> filterUnauthorizedToEditLogicalParent(
      @Nonnull AuthorizationSession session, @Nonnull Map<Urn, Set<Urn>> urnsRequiringEditByChild) {
    if (urnsRequiringEditByChild.isEmpty()) {
      return Set.of();
    }
    return urnsRequiringEditByChild.entrySet().stream()
        .filter(
            entry -> {
              Urn childUrn = entry.getKey();
              Urn proposedParentUrn =
                  entry.getValue().stream()
                      .filter(urn -> !urn.equals(childUrn))
                      .findFirst()
                      .orElse(null);
              return !isAuthorizedToEditLogicalParent(session, childUrn, proposedParentUrn);
            })
        .map(Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * Returns data product URNs whose {@code dataProductProperties.assets} change is not authorized.
   * Authorized when the actor has {@code MANAGE_DATA_PRODUCTS} on at least one product domain
   * (product-side path) or {@code EDIT_ENTITY_DATA_PRODUCTS} on every changed asset (asset-side
   * path). Applies uniformly to additions and removals.
   */
  @Nonnull
  public static Set<Urn> filterUnauthorizedToManageDataProductMembership(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<Urn, Set<Urn>> changedAssetsByProduct) {
    return filterUnauthorizedToManageDataProductMembership(
        operationContext, session, aspectRetriever, changedAssetsByProduct, Map.of());
  }

  /**
   * Like {@link #filterUnauthorizedToManageDataProductMembership(OperationFingerprint,
   * AuthorizationSession, AspectRetriever, Map)} but also considers {@code domains} aspects
   * proposed on data products in the same ingest batch (not yet committed when validation runs).
   */
  @Nonnull
  public static Set<Urn> filterUnauthorizedToManageDataProductMembership(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<Urn, Set<Urn>> changedAssetsByProduct,
      @Nonnull Map<Urn, Aspect> proposedProductDomainsAspects) {
    if (changedAssetsByProduct.isEmpty()) {
      return Set.of();
    }

    Map<Urn, Map<String, Aspect>> persistedProductDomainsAspects =
        aspectRetriever.getLatestAspectObjects(
            operationContext,
            new HashSet<>(changedAssetsByProduct.keySet()),
            Set.of(DOMAINS_ASPECT_NAME));

    return filterUnauthorizedToManageDataProductMembership(
        session,
        changedAssetsByProduct,
        persistedProductDomainsAspects,
        proposedProductDomainsAspects);
  }

  /**
   * Like {@link #filterUnauthorizedToManageDataProductMembership(OperationFingerprint,
   * AuthorizationSession, AspectRetriever, Map, Map)} but uses caller-supplied persisted {@code
   * domains} aspects (avoids a redundant read when membership and rename checks share one fetch).
   */
  @Nonnull
  public static Set<Urn> filterUnauthorizedToManageDataProductMembership(
      @Nonnull AuthorizationSession session,
      @Nonnull Map<Urn, Set<Urn>> changedAssetsByProduct,
      @Nonnull Map<Urn, Map<String, Aspect>> persistedProductDomainsAspects,
      @Nonnull Map<Urn, Aspect> proposedProductDomainsAspects) {
    if (changedAssetsByProduct.isEmpty()) {
      return Set.of();
    }

    Map<Urn, Boolean> assetAuthCache = new HashMap<>();
    return filterUnauthorizedToManageDataProductMembership(
        session,
        changedAssetsByProduct,
        persistedProductDomainsAspects,
        proposedProductDomainsAspects,
        assetAuthCache);
  }

  @Nonnull
  private static Set<Urn> filterUnauthorizedToManageDataProductMembership(
      @Nonnull AuthorizationSession session,
      @Nonnull Map<Urn, Set<Urn>> changedAssetsByProduct,
      @Nonnull Map<Urn, Map<String, Aspect>> persistedProductDomainsAspects,
      @Nonnull Map<Urn, Aspect> proposedProductDomainsAspects,
      @Nonnull Map<Urn, Boolean> assetAuthCache) {
    Set<Urn> unauthorized = new HashSet<>();
    for (Map.Entry<Urn, Set<Urn>> entry : changedAssetsByProduct.entrySet()) {
      Urn dataProductUrn = entry.getKey();
      Set<Urn> changedAssets = entry.getValue();

      Aspect productDomainsAspect = proposedProductDomainsAspects.get(dataProductUrn);
      if (productDomainsAspect == null) {
        productDomainsAspect =
            persistedProductDomainsAspects
                .getOrDefault(dataProductUrn, Map.of())
                .get(DOMAINS_ASPECT_NAME);
      }
      Set<Urn> productDomainUrns = resolveUniqueDomainUrns(productDomainsAspect);

      if (!isAuthorizedToChangeDataProductMembership(
          session, productDomainUrns, changedAssets, assetAuthCache)) {
        unauthorized.add(dataProductUrn);
      }
    }
    return unauthorized;
  }

  /**
   * Returns true when the actor may rename a data product ({@code dataProductProperties.name} or
   * {@code updateName}). Requires {@code MANAGE_DATA_PRODUCTS} on at least one product domain, or
   * {@code EDIT_ENTITY} on the data product itself.
   */
  public static boolean isAuthorizedToRenameDataProduct(
      @Nonnull AuthorizationSession session,
      @Nonnull Urn dataProductUrn,
      @Nonnull Set<Urn> productDomainUrns) {
    if (!productDomainUrns.isEmpty()
        && isAuthorizedToManageDataProductsOnAnyDomain(session, productDomainUrns)) {
      return true;
    }
    return isAuthorizedToEditDataProductEntity(session, dataProductUrn);
  }

  /**
   * Returns data product URNs whose {@code dataProductProperties.name} change is not authorized.
   */
  @Nonnull
  public static Set<Urn> filterUnauthorizedToRenameDataProduct(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Set<Urn> dataProductUrnsWithNameChange,
      @Nonnull Map<Urn, Aspect> proposedProductDomainsAspects) {
    if (dataProductUrnsWithNameChange.isEmpty()) {
      return Set.of();
    }

    Map<Urn, Map<String, Aspect>> persistedProductDomainsAspects =
        aspectRetriever.getLatestAspectObjects(
            operationContext,
            new HashSet<>(dataProductUrnsWithNameChange),
            Set.of(DOMAINS_ASPECT_NAME));

    return filterUnauthorizedToRenameDataProduct(
        session,
        dataProductUrnsWithNameChange,
        persistedProductDomainsAspects,
        proposedProductDomainsAspects);
  }

  /**
   * Like {@link #filterUnauthorizedToRenameDataProduct(OperationFingerprint, AuthorizationSession,
   * AspectRetriever, Set, Map)} but uses caller-supplied persisted {@code domains} aspects.
   */
  @Nonnull
  public static Set<Urn> filterUnauthorizedToRenameDataProduct(
      @Nonnull AuthorizationSession session,
      @Nonnull Set<Urn> dataProductUrnsWithNameChange,
      @Nonnull Map<Urn, Map<String, Aspect>> persistedProductDomainsAspects,
      @Nonnull Map<Urn, Aspect> proposedProductDomainsAspects) {
    if (dataProductUrnsWithNameChange.isEmpty()) {
      return Set.of();
    }

    Set<Urn> unauthorized = new HashSet<>();
    for (Urn dataProductUrn : dataProductUrnsWithNameChange) {
      Aspect productDomainsAspect = proposedProductDomainsAspects.get(dataProductUrn);
      if (productDomainsAspect == null) {
        productDomainsAspect =
            persistedProductDomainsAspects
                .getOrDefault(dataProductUrn, Map.of())
                .get(DOMAINS_ASPECT_NAME);
      }
      Set<Urn> productDomainUrns = resolveUniqueDomainUrns(productDomainsAspect);
      if (!isAuthorizedToRenameDataProduct(session, dataProductUrn, productDomainUrns)) {
        unauthorized.add(dataProductUrn);
      }
    }
    return unauthorized;
  }

  private static boolean isAuthorizedToEditDataProductEntity(
      @Nonnull AuthorizationSession session, @Nonnull Urn dataProductUrn) {
    EntitySpec productSpec =
        new EntitySpec(dataProductUrn.getEntityType(), dataProductUrn.toString());
    return com.datahub.authorization.AuthUtil.isAuthorized(
        session,
        new DisjunctivePrivilegeGroup(ImmutableList.of(ALL_ENTITY_PRIVILEGES)),
        productSpec);
  }

  /**
   * Returns true when the actor may change {@code dataProductProperties.assets} via the
   * product-side path ({@code MANAGE_DATA_PRODUCTS} on any product domain) or the asset-side path
   * ({@code EDIT_ENTITY_DATA_PRODUCTS} on every changed asset).
   */
  public static boolean isAuthorizedToChangeDataProductMembership(
      @Nonnull AuthorizationSession session,
      @Nonnull Set<Urn> productDomainUrns,
      @Nonnull Set<Urn> changedAssetUrns) {
    return isAuthorizedToChangeDataProductMembership(
        session, productDomainUrns, changedAssetUrns, new HashMap<>());
  }

  static boolean isAuthorizedToChangeDataProductMembership(
      @Nonnull AuthorizationSession session,
      @Nonnull Set<Urn> productDomainUrns,
      @Nonnull Set<Urn> changedAssetUrns,
      @Nonnull Map<Urn, Boolean> assetAuthCache) {
    if (changedAssetUrns.isEmpty()) {
      return false;
    }

    if (!productDomainUrns.isEmpty()
        && isAuthorizedToManageDataProductsOnAnyDomain(session, productDomainUrns)) {
      return true;
    }

    return changedAssetUrns.stream()
        .allMatch(
            asset ->
                assetAuthCache.computeIfAbsent(
                    asset, key -> isAuthorizedToEditDataProductMembershipOnAsset(session, key)));
  }

  /**
   * Returns the unique domain URNs associated with a {@link Domains} aspect, preferring {@code
   * domainAssociations} and falling back to the legacy {@code domains} array.
   */
  @Nonnull
  public static Set<Urn> resolveUniqueDomainUrns(@Nullable Domains domains) {
    if (domains == null) {
      return Set.of();
    }

    LinkedHashSet<Urn> uniqueDomainUrns = new LinkedHashSet<>();
    if (domains.hasDomainAssociations() && domains.getDomainAssociations() != null) {
      for (DomainAssociation association : domains.getDomainAssociations()) {
        if (association.hasDomain()) {
          uniqueDomainUrns.add(association.getDomain());
        }
      }
    }
    if (uniqueDomainUrns.isEmpty() && domains.hasDomains() && domains.getDomains() != null) {
      for (Urn domainUrn : domains.getDomains()) {
        if (domainUrn != null) {
          uniqueDomainUrns.add(domainUrn);
        }
      }
    }
    return uniqueDomainUrns;
  }

  @Nonnull
  public static Set<Urn> resolveUniqueDomainUrns(@Nullable Aspect domainsAspect) {
    if (domainsAspect == null) {
      return Set.of();
    }
    return resolveUniqueDomainUrns(
        RecordUtils.toRecordTemplate(Domains.class, domainsAspect.data()));
  }

  public static boolean isAuthorizedToManageDataProductsOnAnyDomain(
      @Nonnull AuthorizationSession session, @Nonnull Collection<Urn> domainUrns) {
    if (domainUrns.isEmpty()) {
      return false;
    }
    for (Urn domainUrn : domainUrns) {
      if (isAuthorizedToManageDataProductsOnDomain(session, domainUrn)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isAuthorizedToManageDataProductsOnDomain(
      @Nonnull AuthorizationSession session, @Nonnull Urn domainUrn) {
    EntitySpec domainSpec = new EntitySpec(domainUrn.getEntityType(), domainUrn.toString());
    return com.datahub.authorization.AuthUtil.isAuthorized(
        session, MANAGE_DATA_PRODUCTS_PRIVILEGES, domainSpec);
  }

  public static boolean isAuthorizedToEditDataProductMembershipOnAsset(
      @Nonnull AuthorizationSession session, @Nonnull Urn assetUrn) {
    EntitySpec assetSpec = new EntitySpec(assetUrn.getEntityType(), assetUrn.toString());
    return com.datahub.authorization.AuthUtil.isAuthorized(
        session, EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGES, assetSpec);
  }

  /**
   * Activation gate for query-read authorization: on by default via {@code
   * authorization.view.queryEntities.enabled} (a missing config block means the default, enabled),
   * and also active under the legacy view-authorization master switch (which carried the original,
   * always-strict query filtering). Disabling the flag is the performance escape valve: query reads
   * then perform no subject lookups at all. Does not override {@code authorization.view.enabled} if
   * enabled.
   */
  public static boolean isQueryViewAuthorizationEnabled(
      @Nonnull io.datahubproject.metadata.context.OperationContext opContext) {
    com.datahub.authorization.config.ViewAuthorizationConfiguration view =
        opContext.getOperationContextConfig().getViewAuthorizationConfiguration();
    return view.getQueryEntities() == null
        || view.getQueryEntities().isEnabled()
        || view.isEnabled();
  }

  /**
   * Resolves the operator-configured {@code requireAllSubjects} mode. With the dedicated flag
   * enabled (the default), the configured mode applies — default {@code COMPAT}. When active only
   * via the legacy view-authorization switch (dedicated flag explicitly disabled), the original
   * all-subjects behavior is preserved ({@code TRUE}, regardless of the configured mode). A missing
   * {@code queryEntities} config block (should not occur outside tests — {@code application.yaml}
   * always declares it) falls back to {@code COMPAT} too, matching the same default.
   */
  private static com.datahub.authorization.config.ViewAuthorizationConfiguration
          .RequireAllSubjectsMode
      requireAllSubjectsMode(
          @Nonnull io.datahubproject.metadata.context.OperationContext opContext) {
    com.datahub.authorization.config.ViewAuthorizationConfiguration view =
        opContext.getOperationContextConfig().getViewAuthorizationConfiguration();
    if (view.getQueryEntities() == null) {
      return com.datahub.authorization.config.ViewAuthorizationConfiguration.RequireAllSubjectsMode
          .COMPAT;
    }
    if (view.getQueryEntities().isEnabled()) {
      return view.getQueryEntities().getRequireAllSubjects();
    }
    return com.datahub.authorization.config.ViewAuthorizationConfiguration.RequireAllSubjectsMode
        .TRUE;
  }

  /**
   * Subject-match mode for ordinary Query-entity-subject checks: direct Query entity reads, {@code
   * listQueries}, REST/OpenAPI reads, and {@link
   * com.linkedin.metadata.authorization.EntityAuthorizationUtils#canViewEntity}'s Query-entity
   * branch (search-result masking, related-entity visibility) all share this one answer. {@code
   * TRUE} and {@code FALSE} apply literally everywhere. {@code COMPAT} resolves to {@code
   * VIEW_AUTHORIZATION_ENABLED}'s current state: require-all when it's on, any-subject when it's
   * off — so turning on VBAC uniformly tightens every one of these paths, and COMPAT is a no-op
   * relative to the old any-subject-everywhere default for deployments that never enable it.
   *
   * <p>Does NOT apply to {@code topSqlQueries} — see {@link
   * #requireAllQuerySubjectsForTopSqlQueries} for that deliberate carve-out.
   */
  public static boolean requireAllQuerySubjects(
      @Nonnull io.datahubproject.metadata.context.OperationContext opContext) {
    com.datahub.authorization.config.ViewAuthorizationConfiguration.RequireAllSubjectsMode mode =
        requireAllSubjectsMode(opContext);
    if (mode
        == com.datahub.authorization.config.ViewAuthorizationConfiguration.RequireAllSubjectsMode
            .COMPAT) {
      return opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled();
    }
    return mode
        == com.datahub.authorization.config.ViewAuthorizationConfiguration.RequireAllSubjectsMode
            .TRUE;
  }

  /**
   * Subject-match mode for {@code topSqlQueries} specifically — deliberately carved out from {@link
   * #requireAllQuerySubjects}. {@code topSqlQueries} entries are bare SQL strings with no recorded
   * per-statement dataset association, unlike a Query entity's {@code querySubjects}, so there is
   * nothing for a require-all check to verify beyond the one containing dataset. {@code TRUE} and
   * {@code FALSE} still apply literally (see {@link #isTopSqlQueriesRestricted}'s use of this for
   * what {@code TRUE} means here), but {@code COMPAT} always resolves to any-subject, regardless of
   * {@code VIEW_AUTHORIZATION_ENABLED} — an actor with {@code VIEW_ENTITY_QUERIES} on a dataset
   * keeps seeing that dataset's own top queries under COMPAT even once VBAC is turned on, without
   * needing the platform-wide {@code VIEW_ALL_QUERIES}.
   */
  public static boolean requireAllQuerySubjectsForTopSqlQueries(
      @Nonnull io.datahubproject.metadata.context.OperationContext opContext) {
    return requireAllSubjectsMode(opContext)
        == com.datahub.authorization.config.ViewAuthorizationConfiguration.RequireAllSubjectsMode
            .TRUE;
  }

  /**
   * Whether the actor holds {@code VIEW_ALL_QUERIES} — the platform-level bypass that grants
   * visibility into every query regardless of subject datasets (see {@link
   * PoliciesConfig#VIEW_ALL_QUERIES_PRIVILEGE}). Checked with no resource spec, since it is a
   * platform privilege. Exposed for batch callers (like this class's own query-set filtering) that
   * need the bypass alone; single-entity callers should prefer {@link
   * #allowedToViewQueriesOnEntity}.
   */
  public static boolean hasViewAllQueriesPrivilege(@Nonnull AuthorizationSession session) {
    return AuthUtil.isAuthorized(session, PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE);
  }

  /**
   * Whether the actor is allowed to view queries associated with this single entity (typically a
   * dataset): granted either via {@link #hasViewAllQueriesPrivilege} or the ordinary {@code
   * VIEW_ENTITY_QUERIES}-family privilege group checked against this entity specifically. Any
   * query-visibility decision keyed to one entity urn should call this rather than checking {@code
   * VIEW_ENTITY_QUERIES} alone, so the {@code VIEW_ALL_QUERIES} bypass — easy to miss, since it's a
   * separate privilege on a separate resource type — is never silently omitted at a future call
   * site.
   */
  public static boolean allowedToViewQueriesOnEntity(
      @Nonnull AuthorizationSession session, @Nonnull Urn entityUrn) {
    if (hasViewAllQueriesPrivilege(session)) {
      return true;
    }
    EntitySpec entitySpec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());
    return AuthUtil.isAuthorized(session, VIEW_ENTITY_QUERIES_PRIVILEGES, entitySpec);
  }

  /**
   * Whether query-derived content (Query entities, {@code topSqlQueries}, view/transform-logic SQL
   * text) tied to {@code entityUrn} may be shown to this actor: granted when query-view
   * authorization is disabled entirely (the {@link #isQueryViewAuthorizationEnabled} escape valve),
   * for system auth, or via {@link #allowedToViewQueriesOnEntity}. Every call site that decides
   * whether to expose SQL for a specific entity should go through this rather than {@link
   * #allowedToViewQueriesOnEntity} alone, so the escape valve and system-auth bypass — each easy to
   * omit independently — are never accidentally missing from a new caller. Requires a full {@code
   * OperationContext} rather than a bare {@link AuthorizationSession} because both bypasses need
   * config/system-auth access the narrower interface doesn't expose.
   */
  public static boolean canViewQueriesOnEntity(
      @Nonnull io.datahubproject.metadata.context.OperationContext opContext,
      @Nonnull Urn entityUrn) {
    return !isQueryViewAuthorizationEnabled(opContext)
        || opContext.isSystemAuth()
        || allowedToViewQueriesOnEntity(opContext, entityUrn);
  }

  /**
   * Whether {@code topSqlQueries} (raw SQL embedded in dataset usage statistics) for {@code
   * resourceUrn} must be withheld from this actor. {@code topSqlQueries} entries are bare strings
   * with no recorded dataset associations, so unlike a Query entity's {@code querySubjects} they
   * cannot be checked per-statement:
   *
   * <ul>
   *   <li>{@link #hasViewAllQueriesPrivilege}: unrestricted in every mode — it grants visibility
   *       into every query platform-wide regardless of subject datasets, so per-statement
   *       association is moot for an actor who already holds it.
   *   <li>Default (any-subject) mode, without {@code VIEW_ALL_QUERIES}: permitted iff the actor
   *       holds {@code VIEW_ENTITY_QUERIES} on {@code resourceUrn} itself — consistent with the
   *       any-subject rule, since every statement in the list ran against this dataset.
   *   <li>Literal {@code TRUE} mode, without {@code VIEW_ALL_QUERIES}: ALWAYS restricted, even for
   *       an actor holding {@code VIEW_ENTITY_QUERIES} on {@code resourceUrn} — a statement may
   *       reference other datasets the actor cannot see, and bare strings carry no way to verify
   *       that. Documented limitation of strict mode. {@code COMPAT} does NOT get this treatment —
   *       see {@link #requireAllQuerySubjectsForTopSqlQueries} for why it stays any-subject here
   *       even once {@code VIEW_AUTHORIZATION_ENABLED} is on.
   * </ul>
   *
   * <p>Uses {@link #isQueryViewAuthorizationEnabled} (not a narrower, dedicated-flag-only check) so
   * this and {@link #canViewQueriesOnEntity} agree on when enforcement is active — a caller with
   * its own inline copy of the enabled-check previously drifted out of sync with the shared one by
   * omitting the legacy {@code VIEW_AUTHORIZATION_ENABLED} activation path.
   */
  public static boolean isTopSqlQueriesRestricted(
      @Nonnull io.datahubproject.metadata.context.OperationContext opContext,
      @Nonnull Urn resourceUrn) {
    if (!isQueryViewAuthorizationEnabled(opContext) || opContext.isSystemAuth()) {
      return false;
    }
    if (requireAllQuerySubjectsForTopSqlQueries(opContext)) {
      return !hasViewAllQueriesPrivilege(opContext);
    }
    return !allowedToViewQueriesOnEntity(opContext, resourceUrn);
  }

  /** Strict-mode overload: every subject dataset must grant the privilege. */
  @Nonnull
  public static Set<Urn> filterViewableQueryEntities(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> queryEntityUrns) {
    return filterViewableQueryEntities(
        operationContext, session, aspectRetriever, queryEntityUrns, true);
  }

  /**
   * Returns Query entity URNs viewable by the actor. In strict mode ({@code requireAllSubjects}),
   * every subject dataset must grant {@code VIEW_ENTITY_QUERIES} (or {@code EDIT_QUERIES} / all
   * privileges, which imply it); otherwise a grant on any single subject dataset suffices. Query
   * entities with no subjects are fail-closed against {@code VIEW_ENTITY_QUERIES} in both modes:
   * that privilege attaches to datasets, so a query with no subjects has nothing to grant against,
   * and no resource-scoped policy — however broad — can satisfy a check against zero resources.
   * {@link PoliciesConfig#VIEW_ALL_QUERIES_PRIVILEGE} is a deliberate, platform-level,
   * unconditional bypass: an actor holding it is returned every query in {@code queryEntityUrns}
   * as-is, subjects or not — it is checked first and short-circuits the per-subject logic entirely,
   * for every query in the batch, not only orphans. Orphans are simply the one case {@code
   * VIEW_ENTITY_QUERIES} could never reach on its own; an actor without {@code VIEW_ALL_QUERIES}
   * still goes through the ordinary subject-derived check below for every query, orphan or not.
   */
  @Nonnull
  public static Set<Urn> filterViewableQueryEntities(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> queryEntityUrns,
      boolean requireAllSubjects) {
    if (queryEntityUrns.isEmpty()) {
      return Set.of();
    }

    if (hasViewAllQueriesPrivilege(session)) {
      return new HashSet<>(queryEntityUrns);
    }

    Map<Urn, Map<String, Aspect>> subjectAspects =
        aspectRetriever.getLatestAspectObjects(
            operationContext, new HashSet<>(queryEntityUrns), Set.of(QUERY_SUBJECTS_ASPECT_NAME));

    Set<Urn> allSubjectUrns = new HashSet<>();
    for (Urn queryUrn : queryEntityUrns) {
      Aspect subjectsAspect =
          subjectAspects.getOrDefault(queryUrn, Map.of()).get(QUERY_SUBJECTS_ASPECT_NAME);
      Set<Urn> subjects = extractSubjectDatasetUrns(subjectsAspect);
      if (subjects.isEmpty()) {
        continue;
      }
      allSubjectUrns.addAll(subjects);
    }

    Set<Urn> readableSubjects =
        allSubjectUrns.isEmpty()
            ? Set.of()
            : filterReadableQuerySubjectDatasets(session, allSubjectUrns);

    Set<Urn> viewableQueries = new HashSet<>();
    for (Urn queryUrn : queryEntityUrns) {
      Aspect subjectsAspect =
          subjectAspects.getOrDefault(queryUrn, Map.of()).get(QUERY_SUBJECTS_ASPECT_NAME);
      Set<Urn> subjects = extractSubjectDatasetUrns(subjectsAspect);
      if (subjects.isEmpty()) {
        continue;
      }
      boolean viewable =
          requireAllSubjects
              ? readableSubjects.containsAll(subjects)
              : subjects.stream().anyMatch(readableSubjects::contains);
      if (viewable) {
        viewableQueries.add(queryUrn);
      }
    }
    return viewableQueries;
  }

  /** Strict-mode overload: every subject dataset must grant the privilege. */
  public static boolean canViewQueryEntity(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn queryEntityUrn) {
    return canViewQueryEntity(operationContext, session, aspectRetriever, queryEntityUrn, true);
  }

  public static boolean canViewQueryEntity(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn queryEntityUrn,
      boolean requireAllSubjects) {
    return filterViewableQueryEntities(
            operationContext, session, aspectRetriever, List.of(queryEntityUrn), requireAllSubjects)
        .contains(queryEntityUrn);
  }

  @Nonnull
  private static Set<Urn> extractSubjectDatasetUrns(@Nullable Aspect querySubjectsAspect) {
    if (querySubjectsAspect == null) {
      return Set.of();
    }
    QuerySubjects querySubjects =
        RecordUtils.toRecordTemplate(QuerySubjects.class, querySubjectsAspect.data());
    if (!querySubjects.hasSubjects()) {
      return Set.of();
    }
    Set<Urn> subjects = new HashSet<>();
    for (QuerySubject subject : querySubjects.getSubjects()) {
      if (!subject.hasEntity()) {
        continue;
      }
      Urn subjectUrn = subject.getEntity();
      if (DATASET_ENTITY_NAME.equals(subjectUrn.getEntityType())) {
        subjects.add(subjectUrn);
      } else if (SCHEMA_FIELD_ENTITY_NAME.equals(subjectUrn.getEntityType())) {
        subjects.add(UrnUtils.getUrn(subjectUrn.getEntityKey().get(0)));
      }
    }
    return subjects;
  }

  @Nonnull
  private static Set<Urn> filterReadableQuerySubjectDatasets(
      @Nonnull AuthorizationSession session, @Nonnull Collection<Urn> datasetUrns) {
    return datasetUrns.stream()
        .filter(urn -> canReadQueryViaSubjectDataset(session, urn))
        .collect(Collectors.toSet());
  }

  private static boolean canReadQueryViaSubjectDataset(
      @Nonnull AuthorizationSession session, @Nonnull Urn datasetUrn) {
    EntitySpec datasetSpec = new EntitySpec(datasetUrn.getEntityType(), datasetUrn.toString());
    return com.datahub.authorization.AuthUtil.isAuthorized(
        session, VIEW_ENTITY_QUERIES_PRIVILEGES, datasetSpec);
  }

  public static boolean isQueryEntity(@Nonnull Urn urn) {
    return QUERY_ENTITY_NAME.equals(urn.getEntityType());
  }
}
