package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.metadata.Constants.APPLICATION_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

import com.datahub.authorization.AuthUtil;
import com.linkedin.datahub.graphql.QueryContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationAuthorizationUtils {

  private ApplicationAuthorizationUtils() {}

  /**
   * Returns true if the current user is authorized to edit any application entity. This is true if
   * the user has either: 1. The EDIT_ENTITY privilege (super user privileges) 2. The
   * EDIT_ENTITY_APPLICATIONS privilege
   */
  public static boolean canManageApplications(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(APPLICATION_ENTITY_NAME));
  }
}
