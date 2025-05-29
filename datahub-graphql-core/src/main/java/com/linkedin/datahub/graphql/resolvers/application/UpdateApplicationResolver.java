package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Application;
import com.linkedin.datahub.graphql.generated.UpdateApplicationInput;
import com.linkedin.datahub.graphql.types.application.mappers.ApplicationMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateApplicationResolver implements DataFetcher<CompletableFuture<Application>> {

  private final ApplicationService _applicationService;

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  @Override
  public CompletableFuture<Application> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateApplicationInput input =
        bindArgument(environment.getArgument("input"), UpdateApplicationInput.class);
    final Urn applicationUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!_applicationService.verifyEntityExists(
              context.getOperationContext(), applicationUrn)) {
            throw new IllegalArgumentException("The Application provided dos not exist");
          }

          Domains domains =
              _applicationService.getApplicationDomains(
                  context.getOperationContext(), applicationUrn);
          if (domains != null && domains.hasDomains() && domains.getDomains().size() > 0) {
            // get first domain since we only allow one domain right now
            Urn domainUrn = UrnUtils.getUrn(domains.getDomains().get(0).toString());
            final DisjunctivePrivilegeGroup orPrivilegeGroup =
                new DisjunctivePrivilegeGroup(ImmutableList.of(ALL_PRIVILEGES_GROUP));
            if (!AuthorizationUtils.isAuthorized(
                context, domainUrn.getEntityType(), domainUrn.toString(), orPrivilegeGroup)) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }
          }

          try {
            final Urn urn =
                _applicationService.updateApplication(
                    context.getOperationContext(),
                    applicationUrn,
                    input.getName(),
                    input.getDescription());
            EntityResponse response =
                _applicationService.getApplicationEntityResponse(
                    context.getOperationContext(), urn);
            if (response != null) {
              return ApplicationMapper.map(context, response);
            }
            // should never happen
            log.error(String.format("Unable to find application with urn %s", applicationUrn));
            return null;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update Application with urn %s", applicationUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
