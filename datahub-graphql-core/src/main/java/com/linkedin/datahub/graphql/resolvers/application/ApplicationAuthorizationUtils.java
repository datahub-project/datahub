package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.metadata.Constants.APPLICATION_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.ApplicationService;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationAuthorizationUtils {

  private ApplicationAuthorizationUtils() {}

  /**
   * Returns true if the current user is authorized to edit any application entity. This is true if
   * the user has the EDIT_ENTITY privilege for applications.
   */
  public static boolean canManageApplications(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(APPLICATION_ENTITY_NAME));
  }

  /**
   * Verifies that the current user is authorized to edit applications on a specific resource
   * entity.
   *
   * @throws AuthorizationException if the user is not authorized
   */
  public static void verifyEditApplicationsAuthorization(
      @Nonnull Urn resourceUrn, @Nonnull QueryContext context) {
    if (!AuthorizationUtils.isAuthorized(
        context,
        resourceUrn.getEntityType(),
        resourceUrn.toString(),
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_APPLICATIONS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType())))))) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
  }

  /**
   * Verifies that all resources exist and that the current user is authorized to edit applications
   * on them.
   *
   * @param resources List of resource URN strings to verify
   * @param applicationService Service to verify entity existence
   * @param context Query context with operation context and authorization info
   * @param operationName Name of the operation being performed (for error messages)
   * @throws RuntimeException if any resource does not exist
   * @throws AuthorizationException if the user is not authorized for any resource
   */
  public static void verifyResourcesExistAndAuthorized(
      @Nonnull List<String> resources,
      @Nonnull ApplicationService applicationService,
      @Nonnull QueryContext context,
      @Nonnull String operationName) {
    for (String resource : resources) {
      Urn resourceUrn = UrnUtils.getUrn(resource);
      if (!applicationService.verifyEntityExists(context.getOperationContext(), resourceUrn)) {
        throw new RuntimeException(
            String.format("Failed to %s, %s in resources does not exist", operationName, resource));
      }
      verifyEditApplicationsAuthorization(resourceUrn, context);
    }
  }
}
