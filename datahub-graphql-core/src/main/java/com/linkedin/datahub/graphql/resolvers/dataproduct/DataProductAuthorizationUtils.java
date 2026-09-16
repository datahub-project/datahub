package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.DataProductService;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataProductAuthorizationUtils {

  private DataProductAuthorizationUtils() {}

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  public static boolean isAuthorizedToUpdateDataProductsForEntity(
      @Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToManageDataProducts(
      @Nonnull QueryContext context, Urn domainUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_DATA_PRODUCTS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, domainUrn.getEntityType(), domainUrn.toString(), orPrivilegeGroups);
  }

  /** Requires {@code MANAGE_DATA_PRODUCTS} on at least one domain associated with the product. */
  public static boolean isAuthorizedToManageDataProductsOnAnyDomain(
      @Nonnull QueryContext context, @Nullable Domains domains) {
    return EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(domains).stream()
        .anyMatch(domainUrn -> isAuthorizedToManageDataProducts(context, domainUrn));
  }

  /**
   * Domain-scoped manage when the product still lists a live Domain; product {@code EDIT_ENTITY} or
   * platform {@code MANAGE_DOMAINS} when every listed Domain is missing or the aspect is empty.
   */
  public static boolean isAuthorizedToManageDataProduct(
      @Nonnull QueryContext context, @Nonnull Urn dataProductUrn, @Nullable Domains domains) {
    Set<Urn> liveDomainUrns = liveDomainUrns(context, domains);
    if (!liveDomainUrns.isEmpty()) {
      return liveDomainUrns.stream()
          .anyMatch(domainUrn -> isAuthorizedToManageDataProducts(context, domainUrn));
    }
    return isAuthorizedToEditDataProduct(context, dataProductUrn)
        || AuthorizationUtils.canManageDomains(context);
  }

  @Nonnull
  private static Set<Urn> liveDomainUrns(@Nonnull QueryContext context, @Nullable Domains domains) {
    Set<Urn> domainUrns = EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(domains);
    if (domainUrns.isEmpty()) {
      return Set.of();
    }
    try {
      AspectRetriever aspectRetriever = context.getOperationContext().getAspectRetriever();
      if (aspectRetriever == null) {
        log.warn(
            "AspectRetriever unavailable; treating listed domains as missing for data product auth");
        return Set.of();
      }
      Map<Urn, Boolean> exists =
          aspectRetriever.entityExists(context.getOperationContext(), domainUrns);
      return domainUrns.stream()
          .filter(urn -> Boolean.TRUE.equals(exists.get(urn)))
          .collect(Collectors.toSet());
    } catch (RuntimeException e) {
      log.warn(
          "Could not resolve live domains for data product auth; treating listed domains as missing",
          e);
      return Set.of();
    }
  }

  /** Product-side membership changes require manage privilege on at least one product domain. */
  public static boolean isAuthorizedToChangeMembershipFromProductSide(
      @Nonnull QueryContext context,
      @Nonnull DataProductService dataProductService,
      @Nonnull Urn dataProductUrn) {
    Domains domains =
        dataProductService.getDataProductDomains(context.getOperationContext(), dataProductUrn);
    return isAuthorizedToManageDataProduct(context, dataProductUrn, domains);
  }

  public static boolean isAuthorizedToEditDataProduct(
      @Nonnull QueryContext context, Urn dataProductUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(ImmutableList.of(ALL_PRIVILEGES_GROUP));

    return AuthorizationUtils.isAuthorized(
        context, dataProductUrn.getEntityType(), dataProductUrn.toString(), orPrivilegeGroups);
  }
}
