package com.linkedin.datahub.graphql.resolvers.integration;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.GetLinkPreviewInput;
import com.linkedin.datahub.graphql.generated.LinkPreviewType;
import com.linkedin.link.LinkPreviewInfo;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetLinkPreviewResolver
    implements DataFetcher<CompletableFuture<com.linkedin.datahub.graphql.generated.LinkPreview>> {

  private final IntegrationsService service;

  public GetLinkPreviewResolver(@Nonnull final IntegrationsService client) {
    this.service = Objects.requireNonNull(client, "service must not be null");
  }

  @Override
  public CompletableFuture<com.linkedin.datahub.graphql.generated.LinkPreview> get(
      DataFetchingEnvironment environment) throws Exception {
    final GetLinkPreviewInput input =
        bindArgument(environment.getArgument("input"), GetLinkPreviewInput.class);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final LinkPreviewInfo result = this.service.getLinkPreview(input.getUrl());
          return result == null ? null : mapResult(result);
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private com.linkedin.datahub.graphql.generated.LinkPreview mapResult(
      @Nonnull final LinkPreviewInfo original) {
    final com.linkedin.datahub.graphql.generated.LinkPreview result =
        new com.linkedin.datahub.graphql.generated.LinkPreview();
    result.setType(LinkPreviewType.valueOf(original.getType().toString()));
    result.setLastRefreshed(original.getLastRefreshedMs());
    result.setJson(original.getJson());
    return result;
  }
}
