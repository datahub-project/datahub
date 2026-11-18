package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.resolvers.application.ApplicationAuthorizationUtils.verifyEditApplicationsAuthorization;

import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.MoveApplicationInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Sets or clears the parent of an Application (applicationProperties.parentApplication). */
@Slf4j
@RequiredArgsConstructor
public class MoveApplicationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final MoveApplicationInput input =
        ResolverUtils.bindArgument(environment.getArgument("input"), MoveApplicationInput.class);
    final QueryContext context = environment.getContext();
    final Urn applicationUrn = UrnUtils.getUrn(input.getResourceUrn());
    final Urn parentUrn =
        input.getParentApplication() != null ? UrnUtils.getUrn(input.getParentApplication()) : null;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          verifyEditApplicationsAuthorization(applicationUrn, context);
          final OperationContext opContext = context.getOperationContext();
          if (!Constants.APPLICATION_ENTITY_NAME.equals(applicationUrn.getEntityType())) {
            throw badRequest("Resource is not an application.");
          }
          if (parentUrn != null) {
            validateParent(opContext, applicationUrn, parentUrn);
          }

          try {
            final ApplicationProperties properties =
                (ApplicationProperties)
                    EntityUtils.getAspectFromEntity(
                        opContext,
                        applicationUrn.toString(),
                        Constants.APPLICATION_PROPERTIES_ASPECT_NAME,
                        entityService,
                        null);
            if (properties == null) {
              throw badRequest("Application properties do not exist.");
            }
            properties.setParentApplication(parentUrn, SetMode.REMOVE_IF_NULL);
            MutationUtils.persistAspect(
                opContext,
                applicationUrn,
                Constants.APPLICATION_PROPERTIES_ASPECT_NAME,
                properties,
                CorpuserUrn.createFromString(context.getActorUrn()),
                entityService);
            return true;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            log.error("Failed to move application {} to parent {}", applicationUrn, parentUrn, e);
            throw new RuntimeException(
                String.format("Failed to move application %s to %s", applicationUrn, parentUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateParent(
      @Nonnull OperationContext opContext, @Nonnull Urn applicationUrn, @Nonnull Urn parentUrn) {
    if (!Constants.APPLICATION_ENTITY_NAME.equals(parentUrn.getEntityType())) {
      throw badRequest("Parent entity is not an application.");
    }
    if (parentUrn.equals(applicationUrn)
        || BoundHierarchyAccess.isDescendant(
            opContext, HierarchyBindings.applicationSpec(opContext), parentUrn, applicationUrn)) {
      throw badRequest("Cannot move an application under itself or one of its descendants.");
    }
    if (!entityService.exists(opContext, parentUrn, true)) {
      throw badRequest("Parent application does not exist.");
    }
  }

  private static DataHubGraphQLException badRequest(@Nonnull String message) {
    return new DataHubGraphQLException(message, DataHubGraphQLErrorCode.BAD_REQUEST);
  }
}
