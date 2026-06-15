package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.DataProductService;
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

  /** Product-side membership changes require manage privilege on at least one product domain. */
  public static boolean isAuthorizedToChangeMembershipFromProductSide(
      @Nonnull QueryContext context,
      @Nonnull DataProductService dataProductService,
      @Nonnull Urn dataProductUrn) {
    Domains domains =
        dataProductService.getDataProductDomains(context.getOperationContext(), dataProductUrn);
    return isAuthorizedToManageDataProductsOnAnyDomain(context, domains);
  }

  /**
   * @deprecated Use {@link #isAuthorizedToManageDataProductsOnAnyDomain}.
   */
  public static boolean isAuthorizedToManageDataProductsOnAllDomains(
      @Nonnull QueryContext context, @Nullable Domains domains) {
    return isAuthorizedToManageDataProductsOnAnyDomain(context, domains);
  }

  public static boolean isAuthorizedToEditDataProduct(
      @Nonnull QueryContext context, Urn dataProductUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(ImmutableList.of(ALL_PRIVILEGES_GROUP));

    return AuthorizationUtils.isAuthorized(
        context, dataProductUrn.getEntityType(), dataProductUrn.toString(), orPrivilegeGroups);
  }
}
