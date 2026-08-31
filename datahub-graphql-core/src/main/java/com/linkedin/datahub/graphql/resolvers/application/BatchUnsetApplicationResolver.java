package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.application.ApplicationAuthorizationUtils.verifyResourcesExistAndAuthorized;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BatchUnsetApplicationInput;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchUnsetApplicationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ApplicationService applicationService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchUnsetApplicationInput input =
        bindArgument(environment.getArgument("input"), BatchUnsetApplicationInput.class);
    final String applicationUrn = input.getApplicationUrn();
    final List<String> resources = input.getResourceUrns();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          verifyResources(resources, context);
          verifyApplication(applicationUrn, context);

          try {
            List<Urn> resourceUrns =
                resources.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
            batchUnsetApplication(applicationUrn, resourceUrns, context);
            return true;
          } catch (Exception e) {
            log.error("Failed to perform update against input {}, {}", input, e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void verifyResources(List<String> resources, QueryContext context) {
    verifyResourcesExistAndAuthorized(resources, applicationService, context, "unset_application");
  }

  private void verifyApplication(String applicationUrn, QueryContext context) {
    if (!applicationService.verifyEntityExists(
        context.getOperationContext(), UrnUtils.getUrn(applicationUrn))) {
      throw new RuntimeException(
          String.format(
              "Failed to batch unset Application, Application urn %s does not exist",
              applicationUrn));
    }
  }

  private void batchUnsetApplication(
      @Nonnull String applicationUrn, List<Urn> resources, QueryContext context) {
    try {
      applicationService.batchUnsetApplication(
          context.getOperationContext(),
          UrnUtils.getUrn(applicationUrn),
          resources,
          UrnUtils.getUrn(context.getActorUrn()));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch unset Application %s from resources with urns %s!",
              applicationUrn, resources),
          e);
    }
  }
}
