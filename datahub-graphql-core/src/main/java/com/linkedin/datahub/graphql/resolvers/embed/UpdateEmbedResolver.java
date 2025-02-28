package com.linkedin.datahub.graphql.resolvers.embed;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Embed;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateEmbedInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.EmbedUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver used for updating the embed render URL for an asset. */
@Slf4j
@RequiredArgsConstructor
public class UpdateEmbedResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateEmbedInput input =
        bindArgument(environment.getArgument("input"), UpdateEmbedInput.class);
    final Urn entityUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!EmbedUtils.isAuthorizedToUpdateEmbedForEntity(entityUrn, environment.getContext())) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          validateUpdateEmbedInput(context.getOperationContext(), input, _entityService);
          try {
            final Embed embed =
                (Embed)
                    EntityUtils.getAspectFromEntity(
                        context.getOperationContext(),
                        entityUrn.toString(),
                        EMBED_ASPECT_NAME,
                        _entityService,
                        new Embed());

            updateEmbed(embed, input);

            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(entityUrn, EMBED_ASPECT_NAME, embed);
            _entityService.ingestProposal(
                context.getOperationContext(),
                proposal,
                new AuditStamp()
                    .setActor(UrnUtils.getUrn(context.getActorUrn()))
                    .setTime(System.currentTimeMillis()),
                false);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to update Embed for to resource with entity urn %s", entityUrn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Validates an instance of {@link UpdateEmbedInput}, and throws an {@link
   * IllegalArgumentException} if the input is not valid.
   *
   * <p>For an input to be valid, the target URN must exist.
   *
   * @param input the input to validate
   * @param entityService an instance of {@link EntityService} used to validate the input.
   */
  private static void validateUpdateEmbedInput(
      @Nonnull OperationContext opContext,
      @Nonnull final UpdateEmbedInput input,
      @Nonnull final EntityService entityService) {
    if (!entityService.exists(opContext, UrnUtils.getUrn(input.getUrn()), true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update embed for entity with urn %s. Entity does not exist!",
              input.getUrn()));
    }
  }

  /**
   * Applies an instance of {@link UpdateEmbedInput} to a base instance of {@link Embed}.
   *
   * @param embed an embed to update
   * @param input the updates to apply
   */
  private static void updateEmbed(
      @Nonnull final Embed embed, @Nonnull final UpdateEmbedInput input) {
    embed.setRenderUrl(input.getRenderUrl(), SetMode.IGNORE_NULL);
  }
}
