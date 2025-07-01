package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchSetDomainInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchSetDomainResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchSetDomainInput input =
        bindArgument(environment.getArgument("input"), BatchSetDomainInput.class);
    final String maybeDomainUrn = input.getDomainUrn();
    final List<ResourceRefInput> resources = input.getResources();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {

          // First, validate the domain
          validateDomain(context.getOperationContext(), maybeDomainUrn);
          validateInputResources(resources, context);

          try {
            // Then execute the bulk add
            batchSetDomains(maybeDomainUrn, resources, context);
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

  private void validateDomain(
      @Nonnull OperationContext opContext, @Nullable String maybeDomainUrn) {
    if (maybeDomainUrn != null) {
      DomainUtils.validateDomain(opContext, UrnUtils.getUrn(maybeDomainUrn), _entityService);
    }
  }

  private void validateInputResources(List<ResourceRefInput> resources, QueryContext context) {
    for (ResourceRefInput resource : resources) {
      validateInputResource(resource, context);
    }
  }

  private void validateInputResource(ResourceRefInput resource, QueryContext context) {
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
    if (!DomainUtils.isAuthorizedToUpdateDomainsForEntity(context, resourceUrn, _entityClient)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    LabelUtils.validateResource(
        context.getOperationContext(),
        resourceUrn,
        resource.getSubResource(),
        resource.getSubResourceType(),
        _entityService);
  }

  private void batchSetDomains(
      String maybeDomainUrn, List<ResourceRefInput> resources, QueryContext context) {
    log.debug("Batch adding Domains. domainUrn: {}, resources: {}", maybeDomainUrn, resources);
    try {
      DomainUtils.setDomainForResources(
          context.getOperationContext(),
          maybeDomainUrn == null ? null : UrnUtils.getUrn(maybeDomainUrn),
          resources,
          UrnUtils.getUrn(context.getActorUrn()),
          _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set Domain %s to resources with urns %s!",
              maybeDomainUrn,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }
}
