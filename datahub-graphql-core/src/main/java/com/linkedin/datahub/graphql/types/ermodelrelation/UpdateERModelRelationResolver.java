package com.linkedin.datahub.graphql.types.ermodelrelation;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.ERModelRelationUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ERModelRelationUpdateInput;
import com.linkedin.datahub.graphql.types.ermodelrelation.mappers.ERModelRelationUpdateInputMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateERModelRelationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final ERModelRelationUpdateInput input =
        bindArgument(environment.getArgument("input"), ERModelRelationUpdateInput.class);
    final String urn = bindArgument(environment.getArgument("urn"), String.class);
    ERModelRelationUrn inputUrn = ERModelRelationUrn.createFromString(urn);
    QueryContext context = environment.getContext();
    final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActorUrn());
    if (!ERModelRelationType.canUpdateERModelRelation(context, inputUrn, input)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            log.debug("Create ERModelRelation input: {}", input);
            final Collection<MetadataChangeProposal> proposals =
                ERModelRelationUpdateInputMapper.map(input, actor);
            proposals.forEach(proposal -> proposal.setEntityUrn(inputUrn));

            try {
              _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
            } catch (RemoteInvocationException e) {
              throw new RuntimeException(
                  String.format("Failed to update erModelRelationship entity"), e);
            }
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update erModelRelationship to resource with input {}, {}",
                input,
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to update erModelRelationship to resource with input %s", input),
                e);
          }
        });
  }
}
