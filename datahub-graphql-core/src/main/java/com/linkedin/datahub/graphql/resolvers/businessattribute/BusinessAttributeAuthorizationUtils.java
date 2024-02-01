package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;

import javax.annotation.Nonnull;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.ALL_PRIVILEGES_GROUP;

public class BusinessAttributeAuthorizationUtils {
    private BusinessAttributeAuthorizationUtils() {

    }

    public static boolean canCreateBusinessAttribute(@Nonnull QueryContext context) {
        final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
            new ConjunctivePrivilegeGroup(ImmutableList.of(
                PoliciesConfig.CREATE_BUSINESS_ATTRIBUTE_PRIVILEGE.getType())),
            new ConjunctivePrivilegeGroup(ImmutableList.of(
                PoliciesConfig.MANAGE_BUSINESS_ATTRIBUTE_PRIVILEGE.getType()))
        ));
        return AuthorizationUtils.isAuthorized(
                context.getAuthorizer(),
                context.getActorUrn(),
                orPrivilegeGroups);
    }

    public static boolean canManageBusinessAttribute(@Nonnull QueryContext context) {
        final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
            new ConjunctivePrivilegeGroup(ImmutableList.of(
                PoliciesConfig.MANAGE_BUSINESS_ATTRIBUTE_PRIVILEGE.getType()))
        ));
        return AuthorizationUtils.isAuthorized(
                context.getAuthorizer(),
                context.getActorUrn(),
                orPrivilegeGroups);
    }

    public static boolean isAuthorizeToUpdateDataset(QueryContext context, Urn targetUrn) {
        final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_DATASET_COL_BUSINESS_ATTRIBUTE_PRIVILEGE.getType()))
        ));

        return AuthorizationUtils.isAuthorized(
                context.getAuthorizer(),
                context.getActorUrn(),
                targetUrn.getEntityType(),
                targetUrn.toString(),
                orPrivilegeGroups);
    }
}
