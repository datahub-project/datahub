package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.InferredColumnDescriptions;
import com.linkedin.datahub.graphql.generated.InferredDocumentation;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.invoker.JSON;
import io.datahubproject.integrations.model.SuggestedDescription;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class InferDocumentationResolver
    implements DataFetcher<CompletableFuture<InferredDocumentation>> {
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<InferredDocumentation> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn targetUrn =
        Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canView(context.getOperationContext(), targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          final SuggestedDescription inferredDocumentation =
              this._integrationsService.inferDocumentation(targetUrn);
          return mapInferredDocumentation(inferredDocumentation);
        });
  }

  private InferredDocumentation mapInferredDocumentation(
      SuggestedDescription inferredDocumentation) {
    final InferredDocumentation result = new InferredDocumentation();
    if (Objects.nonNull(inferredDocumentation.getEntityDescription())) {
      result.setEntityDescription(inferredDocumentation.getEntityDescription());
    }
    if (Objects.nonNull(inferredDocumentation.getColumnDescriptions())) {
      final InferredColumnDescriptions inferredColumnDescriptions =
          new InferredColumnDescriptions();
      inferredColumnDescriptions.setJsonBlob(
          JSON.serialize(inferredDocumentation.getColumnDescriptions()));
      result.setColumnDescriptions(inferredColumnDescriptions);
    }
    return result;
  }
}
