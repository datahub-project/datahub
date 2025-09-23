package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.LinkSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateLinkInput;
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
public class UpdateLinkResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateLinkInput input =
        bindArgument(environment.getArgument("input"), UpdateLinkInput.class);

    String currentLinkUrl = input.getCurrentUrl();
    String currentLinkLabel = input.getCurrentLabel();
    String linkUrl = input.getLinkUrl();
    String linkLabel = input.getLabel();
    LinkSettingsInput settingsInput = input.getSettings();
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    if (!LinkUtils.isAuthorizedToUpdateLinks(context, targetUrn)
        && !GlossaryUtils.canUpdateGlossaryEntity(targetUrn, context, _entityClient)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          LinkUtils.validateUpdateInput(
              context.getOperationContext(), currentLinkUrl, linkUrl, targetUrn, _entityService);
          try {

            log.debug("Updating Link. input: {}", input.toString());

            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            LinkUtils.updateLink(
                context.getOperationContext(),
                currentLinkUrl,
                currentLinkLabel,
                linkUrl,
                linkLabel,
                targetUrn,
                actor,
                settingsInput,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update link to resource with input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to update link to resource with input %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
