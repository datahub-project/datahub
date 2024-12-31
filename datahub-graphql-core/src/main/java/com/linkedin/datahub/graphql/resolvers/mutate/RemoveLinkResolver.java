package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RemoveLinkInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LinkUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveLinkResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final RemoveLinkInput input =
        bindArgument(environment.getArgument("input"), RemoveLinkInput.class);

    String linkUrl = input.getLinkUrl();
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    if (!LinkUtils.isAuthorizedToUpdateLinks(context, targetUrn)
        && !GlossaryUtils.canUpdateGlossaryEntity(targetUrn, context, _entityClient)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          LinkUtils.validateAddRemoveInput(
              context.getOperationContext(), linkUrl, targetUrn, _entityService);
          try {
            log.debug("Removing Link input: {}", input);

            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            LinkUtils.removeLink(
                context.getOperationContext(), linkUrl, targetUrn, actor, _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to remove link from resource with input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to remove link from resource with input  %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
