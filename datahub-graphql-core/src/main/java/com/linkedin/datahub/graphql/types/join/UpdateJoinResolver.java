package com.linkedin.datahub.graphql.types.join;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.JoinUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.JoinUpdateInput;
import com.linkedin.datahub.graphql.types.join.mappers.JoinUpdateInputMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


@Slf4j
@RequiredArgsConstructor
public class UpdateJoinResolver implements DataFetcher<CompletableFuture<Boolean>> {

    private final EntityClient _entityClient;
    @Override
    public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
        final JoinUpdateInput input = bindArgument(environment.getArgument("input"), JoinUpdateInput.class);
        final String urn = bindArgument(environment.getArgument("urn"), String.class);
        JoinUrn inputUrn = JoinUrn.createFromString(urn);
        QueryContext context = environment.getContext();
        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActorUrn());
        if (!JoinType.canUpdateJoin(context, inputUrn, input)) {
            throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("Create Join input: {}", input);
                final Collection<MetadataChangeProposal> proposals = JoinUpdateInputMapper.map(input, actor);
                proposals.forEach(proposal -> proposal.setEntityUrn(inputUrn));

                try {
                    _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
                } catch (RemoteInvocationException e) {
                    throw new RuntimeException(String.format("Failed to update join entity"), e);
                }
                return true;
            } catch (Exception e) {
                log.error("Failed to update Join to resource with input {}, {}", input, e.getMessage());
                throw new RuntimeException(String.format("Failed to update join to resource with input %s", input), e);
            }
        });
    }
}
