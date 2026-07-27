package com.linkedin.datahub.graphql.resolvers.assertion;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;

public class AssertionUtils {
  public static boolean isAuthorizedToEditAssertionFromAssertee(
      final QueryContext context, final Urn asserteeUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                AuthorizationUtils.ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_ASSERTIONS_PRIVILEGE.getType()))));
    return AuthorizationUtils.isAuthorized(
        context, asserteeUrn.getEntityType(), asserteeUrn.toString(), orPrivilegeGroups);
  }

  public static Urn getAsserteeUrnFromInfo(final AssertionInfo info) {
    switch (info.getType()) {
      case DATASET:
        return info.getDatasetAssertion().getDataset();
      case FRESHNESS:
        return info.getFreshnessAssertion().getEntity();
      case VOLUME:
        return info.getVolumeAssertion().getEntity();
      case SQL:
        return info.getSqlAssertion().getEntity();
      case FIELD:
        return info.getFieldAssertion().getEntity();
      case DATA_SCHEMA:
        return info.getSchemaAssertion().getEntity();
      case CUSTOM:
        return info.getCustomAssertion().getEntity();
      default:
        throw new RuntimeException(
            String.format("Unsupported Assertion Type %s provided", info.getType()));
    }
  }
}
