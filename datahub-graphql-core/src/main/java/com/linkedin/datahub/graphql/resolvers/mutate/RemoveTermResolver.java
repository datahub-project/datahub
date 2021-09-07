package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.TermUpdateInput;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


public class RemoveTermResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private static final Logger _logger = LoggerFactory.getLogger(MutableTypeResolver.class.getName());

  private EntityService _entityService;

  public RemoveTermResolver(EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final TermUpdateInput input = bindArgument(environment.getArgument("input"), TermUpdateInput.class);
    Urn termUrn = Urn.createFromString(input.getTermUrn());
    Urn targetUrn = Urn.createFromString(input.getTargetUrn());

    if (!LabelUtils.isAuthorizedToUpdateTerms(environment.getContext(), targetUrn, input.getSubResource())) {
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(() -> {
      try {

        if (!termUrn.getEntityType().equals("glossaryTerm")) {
          _logger.error(String.format("Failed to remove %s. It is not a glossary term urn.", termUrn.toString()));
          return false;
        }

        _logger.info(String.format("Removing Term. input: %s", input));
        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActor());
        LabelUtils.removeTermFromTarget(
            termUrn,
            targetUrn,
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
