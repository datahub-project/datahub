package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.Optional;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;


public class TestUtils {

  /**
   * Returns true if the authenticated user is able to manage tests.
   */
  public static boolean canManageTests(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_TESTS_PRIVILEGE);
  }

  private TestUtils() { }
}
