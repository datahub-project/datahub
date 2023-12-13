package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class SuggestDescriptionResolver implements DataFetcher<CompletableFuture<String>> {
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn targetUrn =
        Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canViewEntity(targetUrn, context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          var suggestedDescription = this._integrationsService.suggestDescription(targetUrn);

          // TODO: Not sure why I need this cast here, since it's declared as a String
          // in the Python Pydantic model.
          return (String) suggestedDescription.getEntityDescription();
        });
  }
}
