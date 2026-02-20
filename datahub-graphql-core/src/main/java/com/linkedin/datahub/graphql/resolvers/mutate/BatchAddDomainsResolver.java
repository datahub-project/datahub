package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchAddDomainsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.SiblingsUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchAddDomainsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchAddDomainsInput input =
        bindArgument(environment.getArgument("input"), BatchAddDomainsInput.class);
    final List<Urn> domainUrns =
        input.getDomainUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    final List<ResourceRefInput> resources = input.getResources();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          validateDomains(context.getOperationContext(), domainUrns);

          if (resources.size() == 1 && resources.get(0).getSubResource() != null) {
            return handleAddDomainsToSingleSchemaField(context, resources, domainUrns);
          }

          validateInputResources(resources, context, domainUrns);

          try {
            batchAddDomains(domainUrns, resources, context);
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

  private Boolean handleAddDomainsToSingleSchemaField(
      @Nonnull final QueryContext context,
      @Nonnull final List<ResourceRefInput> resources,
      @Nonnull final List<Urn> domainUrns) {
    final ResourceRefInput resource = resources.get(0);
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
    final List<Urn> siblingUrns =
        SiblingsUtils.getSiblingUrns(context.getOperationContext(), resourceUrn, _entityService);
    return attemptBatchAddDomainsWithSiblings(
        domainUrns, resource, context, new HashSet<>(), siblingUrns);
  }

  private Boolean attemptBatchAddDomainsWithSiblings(
      @Nonnull final List<Urn> domainUrns,
      @Nonnull final ResourceRefInput resource,
      @Nonnull final QueryContext context,
      @Nonnull final HashSet<Urn> attemptedUrns,
      @Nonnull final List<Urn> siblingUrns) {
    attemptedUrns.add(UrnUtils.getUrn(resource.getResourceUrn()));
    final List<ResourceRefInput> resources = new ArrayList<>();
    resources.add(resource);

    try {
      validateInputResources(resources, context, domainUrns);
      batchAddDomains(domainUrns, resources, context);
      return true;
    } catch (Exception e) {
      final Optional<Urn> siblingUrn = SiblingsUtils.getNextSiblingUrn(siblingUrns, attemptedUrns);

      if (siblingUrn.isPresent()) {
        log.warn(
            "Failed to add domains for resourceUrn {} and subResource {}, trying sibling urn {} now.",
            resource.getResourceUrn(),
            resource.getSubResource(),
            siblingUrn.get());
        resource.setResourceUrn(siblingUrn.get().toString());
        return attemptBatchAddDomainsWithSiblings(
            domainUrns, resource, context, attemptedUrns, siblingUrns);
      } else {
        log.error(
            "Failed to perform update against resource {}, {}",
            resource.toString(),
            e.getMessage());
        throw new RuntimeException(
            String.format("Failed to perform update against resource %s", resource.toString()), e);
      }
    }
  }

  private void validateDomains(@Nonnull OperationContext opContext, List<Urn> domainUrns) {
    for (Urn domainUrn : domainUrns) {
      DomainUtils.validateDomain(opContext, domainUrn, _entityService);
    }
  }

  private void validateInputResources(
      List<ResourceRefInput> resources, QueryContext context, List<Urn> domainUrns) {
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

    if (!_entityService.exists(context.getOperationContext(), resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to add Domains to Entity %s. Entity does not exist.", resourceUrn));
    }
  }

  private void batchAddDomains(
      List<Urn> domainUrns, List<ResourceRefInput> resources, QueryContext context) {
    log.debug("Batch adding Domains. domains: {}, resources: {}", domainUrns, resources);
    try {
      DomainUtils.addDomainsToResources(
          context.getOperationContext(),
          domainUrns,
          resources,
          UrnUtils.getUrn(context.getActorUrn()),
          _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Domains %s to resources with urns %s!",
              domainUrns,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }
}
