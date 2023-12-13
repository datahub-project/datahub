package com.linkedin.datahub.graphql.resolvers.connection;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;

/** Utilities for working with DataHub Connections. */
public class ConnectionUtils {

  /**
   * Returns true if the user is able to read and or write connection between DataHub and external
   * platforms.
   */
  public static boolean canManageConnections(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_CONNECTIONS_PRIVILEGE.getType()))));
    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  private ConnectionUtils() {}
}
