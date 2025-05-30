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
import com.linkedin.datahub.graphql.generated.CreateApplicationInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.datahub.graphql.types.application.mappers.ApplicationMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateApplicationResolver implements DataFetcher<CompletableFuture<Application>> {

  private final ApplicationService _applicationService;
  private final EntityService _entityService;

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  @Override
  public CompletableFuture<Application> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final CreateApplicationInput input =
        bindArgument(environment.getArgument("input"), CreateApplicationInput.class);
    final Authentication authentication = context.getAuthentication();
    final Urn domainUrn = UrnUtils.getUrn(input.getDomainUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!_applicationService.verifyEntityExists(context.getOperationContext(), domainUrn)) {
            throw new IllegalArgumentException("The Domain provided dos not exist");
          }

          final DisjunctivePrivilegeGroup orPrivilegeGroup =
              new DisjunctivePrivilegeGroup(ImmutableList.of(ALL_PRIVILEGES_GROUP));

          if (!AuthorizationUtils.isAuthorized(
              context, domainUrn.getEntityType(), domainUrn.toString(), orPrivilegeGroup)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            final Urn applicationUrn =
                _applicationService.createApplication(
                    context.getOperationContext(),
                    input.getId(),
                    input.getProperties().getName(),
                    input.getProperties().getDescription());
            _applicationService.setDomain(
                context.getOperationContext(),
                applicationUrn,
                UrnUtils.getUrn(input.getDomainUrn()));
            OwnerUtils.addCreatorAsOwner(
                context, applicationUrn.toString(), OwnerEntityType.CORP_USER, _entityService);
            EntityResponse response =
                _applicationService.getApplicationEntityResponse(
                    context.getOperationContext(), applicationUrn);
            if (response != null) {
              return ApplicationMapper.map(context, response);
            }
            // should never happen
            log.error(String.format("Unable to find application with urn %s", applicationUrn));
            return null;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to create a new Application from input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
