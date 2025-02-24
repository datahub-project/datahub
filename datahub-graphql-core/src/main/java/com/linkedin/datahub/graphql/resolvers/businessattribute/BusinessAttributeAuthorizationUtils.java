package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;

public class BusinessAttributeAuthorizationUtils {
  private BusinessAttributeAuthorizationUtils() {}

  public static boolean canCreateBusinessAttribute(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.CREATE_BUSINESS_ATTRIBUTE_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.MANAGE_BUSINESS_ATTRIBUTE_PRIVILEGE.getType()))));
    return AuthUtil.isAuthorized(context.getOperationContext(), orPrivilegeGroups, null);
  }

  public static boolean canManageBusinessAttribute(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.MANAGE_BUSINESS_ATTRIBUTE_PRIVILEGE.getType()))));
    return AuthUtil.isAuthorized(context.getOperationContext(), orPrivilegeGroups, null);
  }
}
