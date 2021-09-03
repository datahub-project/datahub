package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.datahub.graphql.generated.LabelUpdateInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


public class RemoveLabelResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private static final Logger _logger = LoggerFactory.getLogger(MutableTypeResolver.class.getName());

  public RemoveLabelResolver() { }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final LabelUpdateInput input = bindArgument(environment.getArgument("input"), LabelUpdateInput.class);

    return CompletableFuture.supplyAsync(() -> {
      try {
        _logger.debug(String.format("Removing Label. input: %s", input));
        return LabelUtils.removeLabelFromEntity(
            input.getLabelUrn(),
            input.getTargetUrn(),
            input.getSubResource(),
            environment.getContext()
        );
      } catch (Exception e) {
        _logger.error(String.format("Failed to perform update against input %s", input.toString()) + " " + e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }
}
