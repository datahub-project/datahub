package com.linkedin.datahub.graphql.resolvers.dimension;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.service.DimensionTypeService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteDimensionNameResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DimensionTypeService _dimensionTypeService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String dimensionNameUrn = environment.getArgument("urn");
    final Urn urn = UrnUtils.getUrn(dimensionNameUrn);
    // By default, delete references
    final boolean deleteReferences =
        environment.getArgument("deleteReferences") == null
            ? true
            : environment.getArgument("deleteReferences");

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            _dimensionTypeService.deleteDimensionType(
                context.getOperationContext(), urn, deleteReferences);
            log.info(String.format("Successfully deleted dimension name %s with urn", urn));
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete dimension name with urn %s", dimensionNameUrn), e);
          }
        });
  }
}
