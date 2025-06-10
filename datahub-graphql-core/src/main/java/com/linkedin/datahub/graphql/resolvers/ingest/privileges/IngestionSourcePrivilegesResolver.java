package com.linkedin.datahub.graphql.resolvers.ingest.privileges;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourcePrivileges;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

public class IngestionSourcePrivilegesResolver
    implements DataFetcher<CompletableFuture<IngestionSourcePrivileges>> {

  public IngestionSourcePrivilegesResolver() {}

  @Override
  public CompletableFuture<IngestionSourcePrivileges> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urnString = ((IngestionSource) environment.getSource()).getUrn();
    final Urn urn = UrnUtils.getUrn(urnString);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final IngestionSourcePrivileges ingestionSourcePrivileges =
              new IngestionSourcePrivileges();
          ingestionSourcePrivileges.setCanEdit(IngestionAuthUtils.canEditIngestion(context, urn));
          ingestionSourcePrivileges.setCanView(IngestionAuthUtils.canViewIngestion(context, urn));
          ingestionSourcePrivileges.setCanDelete(
              IngestionAuthUtils.canDeleteIngestion(context, urn));
          ingestionSourcePrivileges.setCanExecute(
              IngestionAuthUtils.canExecuteIngestion(context, urn));
          return ingestionSourcePrivileges;
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
