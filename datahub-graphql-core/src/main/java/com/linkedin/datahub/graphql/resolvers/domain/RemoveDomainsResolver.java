package com.linkedin.datahub.graphql.resolvers.domain;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for removing multiple Domains from a Metadata Asset. Preserves other domains.
 * Requires the EDIT_DOMAINS privilege for a particular asset.
 */
@Slf4j
@RequiredArgsConstructor
public class RemoveDomainsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final Urn entityUrn = Urn.createFromString(environment.getArgument("entityUrn"));
    final List<String> domainUrnStrings = environment.getArgument("domainUrns");
    final List<Urn> domainUrns =
        domainUrnStrings.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DomainUtils.isAuthorizedToUpdateDomainsForEntity(
              environment.getContext(), entityUrn, _entityClient)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          validateRemoveDomainsInput(context, entityUrn);

          try {
            final List<ResourceRefInput> resources = new ArrayList<>();
            final ResourceRefInput resource = new ResourceRefInput();
            resource.setResourceUrn(entityUrn.toString());
            resources.add(resource);

            DomainUtils.removeDomainsFromResources(
                context.getOperationContext(),
                domainUrns,
                resources,
                UrnUtils.getUrn(context.getActorUrn()),
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to remove Domains from resource with entity urn {}, domain urns {}: {}",
                entityUrn,
                domainUrns,
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to remove Domains from resource with entity urn %s, domain urns %s",
                    entityUrn, domainUrns),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateRemoveDomainsInput(QueryContext context, Urn entityUrn) {
    if (!_entityService.exists(context.getOperationContext(), entityUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to remove Domains from Entity %s. Entity does not exist.", entityUrn));
    }
  }
}
