package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.TagUpdateInput;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


public class AddTagResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private static final Logger _logger = LoggerFactory.getLogger(MutableTypeResolver.class.getName());

  private EntityService _entityService;

  public AddTagResolver(EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final TagUpdateInput input = bindArgument(environment.getArgument("input"), TagUpdateInput.class);

    return CompletableFuture.supplyAsync(() -> {
      try {
        Urn tagUrn = Urn.createFromString(input.getTagUrn());
        if (!tagUrn.getEntityType().equals("tag")) {
          _logger.error(String.format("Failed to add %s. It is not a tag urn.", tagUrn.toString()));
          return false;
        }

        _logger.info(String.format("Adding Tag. input: %s", input));
        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActor());
        LabelUtils.addTagToTarget(
            tagUrn,
            Urn.createFromString(input.getTargetUrn()),
            input.getSubResource(),
            actor,
            _entityService
        );
        return true;
      } catch (Exception e) {
        _logger.error(String.format("Failed to perform update against input %s", input.toString()) + " " + e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }
}
