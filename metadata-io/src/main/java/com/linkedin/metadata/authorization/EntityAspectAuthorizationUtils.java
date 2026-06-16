package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.QUERY_SUBJECTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;

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

  private static final DisjunctivePrivilegeGroup EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              ALL_ENTITY_PRIVILEGES,
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGE.getType()))));

  private EntityAspectAuthorizationUtils() {}

  /**
   * Returns child URNs whose {@code logicalParent} write is unauthorized. The actor must have
   * {@code EDIT_ENTITY} on the child entity and, when a parent is being set, on the proposed parent
   * as well.
   */
  @Nonnull
  public static Set<Urn> filterUnauthorizedToEditLogicalParent(
      @Nonnull AuthorizationSession session, @Nonnull Map<Urn, Set<Urn>> urnsRequiringEditByChild) {
    if (urnsRequiringEditByChild.isEmpty()) {
      return Set.of();
    }
    return urnsRequiringEditByChild.entrySet().stream()
        .filter(
            entry ->
                !com.datahub.authorization.AuthUtil.isAuthorizedEntityUrns(
                    session, ApiOperation.UPDATE, entry.getValue()))
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

  @Nonnull
  private static Set<Urn> filterUnauthorizedToManageDataProductMembership(
      @Nonnull AuthorizationSession session,
      @Nonnull Map<Urn, Set<Urn>> changedAssetsByProduct,
      @Nonnull Map<Urn, Map<String, Aspect>> persistedProductDomainsAspects,
      @Nonnull Map<Urn, Aspect> proposedProductDomainsAspects) {
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

      if (!isAuthorizedToChangeDataProductMembership(session, productDomainUrns, changedAssets)) {
        unauthorized.add(dataProductUrn);
      }
    }
    return unauthorized;
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
    if (changedAssetUrns.isEmpty()) {
      return false;
    }

    boolean productSide =
        !productDomainUrns.isEmpty()
            && isAuthorizedToManageDataProductsOnAnyDomain(session, productDomainUrns);

    boolean assetSide =
        changedAssetUrns.stream()
            .allMatch(asset -> isAuthorizedToEditDataProductMembershipOnAsset(session, asset));

    return productSide || assetSide;
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
   * Returns Query entity URNs viewable by the actor: every subject dataset must be readable via
   * {@code VIEW_ENTITY_PAGE} or editable via {@code EDIT_ENTITY_QUERIES} (or {@code EDIT_ENTITY}).
   * Query entities with no subjects are denied (fail-closed).
   */
  @Nonnull
  public static Set<Urn> filterViewableQueryEntities(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> queryEntityUrns) {
    if (queryEntityUrns.isEmpty()) {
      return Set.of();
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
      if (readableSubjects.containsAll(subjects)) {
        viewableQueries.add(queryUrn);
      }
    }
    return viewableQueries;
  }

  public static boolean canViewQueryEntity(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn queryEntityUrn) {
    return filterViewableQueryEntities(
            operationContext, session, aspectRetriever, List.of(queryEntityUrn))
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
    if (com.datahub.authorization.AuthUtil.canViewEntity(session, datasetUrn)) {
      return true;
    }
    EntitySpec datasetSpec = new EntitySpec(datasetUrn.getEntityType(), datasetUrn.toString());
    return com.datahub.authorization.AuthUtil.isAuthorized(
        session, EDIT_ENTITY_QUERIES_PRIVILEGES, datasetSpec);
  }

  public static boolean isQueryEntity(@Nonnull Urn urn) {
    return QUERY_ENTITY_NAME.equals(urn.getEntityType());
  }
}
