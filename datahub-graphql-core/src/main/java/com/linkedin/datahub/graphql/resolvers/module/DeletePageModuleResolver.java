package com.linkedin.datahub.graphql.resolvers.module;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DeletePageModuleInput;
import com.linkedin.metadata.service.PageModuleService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeletePageModuleResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final PageModuleService _pageModuleService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final DeletePageModuleInput input =
        bindArgument(environment.getArgument("input"), DeletePageModuleInput.class);

    final String moduleUrn = input.getUrn();
    final Urn urn = UrnUtils.getUrn(moduleUrn);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _pageModuleService.deletePageModule(context.getOperationContext(), urn);
            log.info(String.format("Successfully deleted PageModule with urn %s", moduleUrn));
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete PageModule with urn %s", moduleUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
