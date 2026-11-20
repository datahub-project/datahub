package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canViewRelationship;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.metadata.Constants.APPLICATION_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Application;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentApplicationsResult;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Resolves the full chain of parent applications for an Application, direct parent first
 * (app-of-apps hierarchy), by walking the ApplicationPartOf relationship.
 */
public class ParentApplicationsResolver
    implements DataFetcher<CompletableFuture<ParentApplicationsResult>> {

  @Override
  public CompletableFuture<ParentApplicationsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = getQueryContext(environment);
    final Urn urn = UrnUtils.getUrn(((Application) environment.getSource()).getUrn());

    if (!APPLICATION_ENTITY_NAME.equals(urn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Failed to resolve parent applications for entity %s", urn));
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> parentUrns =
                BoundHierarchyAccess.orderedParents(
                    context.getOperationContext(),
                    HierarchyBindings.applicationSpec(context.getOperationContext()),
                    urn,
                    context.getMaxParentDepth());

            List<Application> viewableApplications =
                parentUrns.stream()
                    .filter(
                        parentUrn ->
                            canViewRelationship(context.getOperationContext(), parentUrn, urn))
                    .map(
                        parentUrn -> {
                          final Application application = new Application();
                          application.setUrn(parentUrn.toString());
                          application.setType(EntityType.APPLICATION);
                          return application;
                        })
                    .collect(Collectors.toList());

            final ParentApplicationsResult result = new ParentApplicationsResult();
            result.setCount(viewableApplications.size());
            result.setApplications(viewableApplications);
            return result;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to load parent applications for entity %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
