package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.application.ApplicationAuthorizationUtils.verifyResourcesExistAndAuthorized;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BatchSetApplicationInput;
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
public class BatchSetApplicationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ApplicationService applicationService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchSetApplicationInput input =
        bindArgument(environment.getArgument("input"), BatchSetApplicationInput.class);
    final String maybeApplicationUrn = input.getApplicationUrn();
    final List<String> resources = input.getResourceUrns();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          verifyResources(resources, context);
          verifyApplication(maybeApplicationUrn, context);

          try {
            List<Urn> resourceUrns =
                resources.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
            if (maybeApplicationUrn != null) {
              batchSetApplication(maybeApplicationUrn, resourceUrns, context);
            } else {
              batchUnsetApplication(resourceUrns, context);
            }
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void verifyResources(List<String> resources, QueryContext context) {
    verifyResourcesExistAndAuthorized(resources, applicationService, context, "set_application");
  }

  private void verifyApplication(String maybeApplicationUrn, QueryContext context) {
    if (maybeApplicationUrn != null
        && !applicationService.verifyEntityExists(
            context.getOperationContext(), UrnUtils.getUrn(maybeApplicationUrn))) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set Application, Application urn %s does not exist",
              maybeApplicationUrn));
    }
  }

  private void batchSetApplication(
      @Nonnull String applicationUrn, List<Urn> resources, QueryContext context) {
    log.debug(
        "Batch setting Application. application urn: {}, resources: {}", applicationUrn, resources);
    try {
      applicationService.batchSetApplicationAssets(
          context.getOperationContext(),
          UrnUtils.getUrn(applicationUrn),
          resources,
          UrnUtils.getUrn(context.getActorUrn()));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set Application %s to resources with urns %s!",
              applicationUrn, resources),
          e);
    }
  }

  private void batchUnsetApplication(List<Urn> resources, QueryContext context) {
    log.debug("Batch unsetting Application. resources: {}", resources);
    try {
      for (Urn resource : resources) {
        applicationService.unsetApplication(
            context.getOperationContext(), resource, UrnUtils.getUrn(context.getActorUrn()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to batch unset application for resources with urns %s!", resources),
          e);
    }
  }
}
