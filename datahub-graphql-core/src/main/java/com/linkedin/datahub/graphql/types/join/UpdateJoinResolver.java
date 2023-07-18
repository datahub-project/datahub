package com.linkedin.datahub.graphql.types.join;

import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.JoinUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.JoinFieldMappingInput;
import com.linkedin.datahub.graphql.generated.JoinUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.join.mappers.JoinUpdateInputMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.join.FieldMap;
import com.linkedin.join.FieldMapArray;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.JOIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.JOIN_PROPERTIES_ASPECT_NAME;


@Slf4j
@RequiredArgsConstructor
public class UpdateJoinResolver implements DataFetcher<CompletableFuture<Boolean>> {

    private final EntityClient _entityClient;
    @Override
    public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
        final JoinUpdateInput input = bindArgument(environment.getArgument("input"), JoinUpdateInput.class);
        JoinUrn inputUrn = new JoinUrn(UUID.randomUUID().toString());
        QueryContext context = environment.getLocalContext();
        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getAuthentication().getActor().toUrnStr());

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
