package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.MoveDomainInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MoveDomainResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final MoveDomainInput input =
        ResolverUtils.bindArgument(environment.getArgument("input"), MoveDomainInput.class);
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(input.getResourceUrn());
    final Urn newParentDomainUrn =
        input.getParentDomain() != null ? UrnUtils.getUrn(input.getParentDomain()) : null;

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageDomains(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            if (!resourceUrn.getEntityType().equals(Constants.DOMAIN_ENTITY_NAME)) {
              throw new IllegalArgumentException("Resource is not a domain.");
            }

            DomainProperties properties =
                (DomainProperties)
                    EntityUtils.getAspectFromEntity(
                        resourceUrn.toString(),
                        Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
                        _entityService,
                        null);

            if (properties == null) {
              throw new IllegalArgumentException("Domain properties do not exist.");
            }

            if (newParentDomainUrn != null) {
              if (!newParentDomainUrn.getEntityType().equals(Constants.DOMAIN_ENTITY_NAME)) {
                throw new IllegalArgumentException("Parent entity is not a domain.");
              }
              if (!_entityService.exists(newParentDomainUrn)) {
                throw new IllegalArgumentException("Parent entity does not exist.");
              }
            }

            if (DomainUtils.hasNameConflict(
                properties.getName(), newParentDomainUrn, context, _entityClient)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "\"%s\" already exists in the destination domain. Please pick a unique name.",
                      properties.getName()),
                  DataHubGraphQLErrorCode.CONFLICT);
            }

            properties.setParentDomain(newParentDomainUrn, SetMode.REMOVE_IF_NULL);
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            MutationUtils.persistAspect(
                resourceUrn,
                Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
                properties,
                actor,
                _entityService);
            return true;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            log.error(
                "Failed to move domain {} to parent {} : {}",
                input.getResourceUrn(),
                input.getParentDomain(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to move domain %s to %s",
                    input.getResourceUrn(), input.getParentDomain()),
                e);
          }
        });
  }
}
