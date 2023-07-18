package com.linkedin.datahub.graphql.types.join;

import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.CorpuserUrn;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.JoinUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.join.mappers.JoinUpdateInputMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.join.FieldMap;
import com.linkedin.join.FieldMapArray;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.JOIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.JOIN_PROPERTIES_ASPECT_NAME;


@Slf4j
@RequiredArgsConstructor
public class CreateJoinResolver implements DataFetcher<CompletableFuture<Boolean>> {

    private final EntityClient _entityClient;
    public Collection<MetadataChangeProposal> createJoinProposals(JoinUpdateInput input, Urn actor) {
        final Collection<MetadataChangeProposal> proposals = new ArrayList<>(8);
        final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(JOIN_ENTITY_NAME);
        final long currentTime = System.currentTimeMillis();
        final TimeStamp timestamp = new TimeStamp();
        timestamp.setActor(actor, SetMode.IGNORE_NULL);
        timestamp.setTime(currentTime);
        if (input.getProperties() != null) {
            com.linkedin.join.JoinProperties joinProperties = new com.linkedin.join.JoinProperties();
            if (input.getProperties().getName() != null) {
                joinProperties.setName(input.getProperties().getName());
            }
            try {
                if (input.getProperties().getDataSetA() != null) {
                    joinProperties.setDatasetA(DatasetUrn.createFromString(input.getProperties().getDataSetA()));
                }
                if (input.getProperties().getDatasetB() != null) {
                    joinProperties.setDatasetB(DatasetUrn.createFromString(input.getProperties().getDatasetB()));
                }
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            if (input.getProperties().getJoinFieldmappings() != null) {
                JoinFieldMappingInput joinFieldMapping = input.getProperties().getJoinFieldmappings();
                if (joinFieldMapping.getDetails() != null || (joinFieldMapping.getFieldMapping() != null && joinFieldMapping.getFieldMapping().size() > 0)) {
                    com.linkedin.join.JoinFieldMapping joinFieldMapping1 = new com.linkedin.join.JoinFieldMapping();
                    if (joinFieldMapping.getDetails() != null) {
                        joinFieldMapping1.setDetails(joinFieldMapping.getDetails());
                    }

                    if (joinFieldMapping.getFieldMapping() != null && joinFieldMapping.getFieldMapping().size() > 0) {
                        com.linkedin.join.FieldMapArray fieldMapArray = new FieldMapArray();
                        joinFieldMapping.getFieldMapping().forEach(fieldMappingInput -> {
                            com.linkedin.join.FieldMap fieldMap = new FieldMap();
                            if (fieldMappingInput.getAfield() != null) {
                                fieldMap.setAfield(fieldMappingInput.getAfield());
                            }
                            if (fieldMappingInput.getBfield() != null) {
                                fieldMap.setBfield(fieldMappingInput.getBfield());
                            }
                            fieldMapArray.add(fieldMap);
                        });
                        joinFieldMapping1.setFieldMapping(fieldMapArray);
                    }
                    joinProperties.setJoinFieldMappings(joinFieldMapping1);
                }
                if (input.getProperties().getCreated() != null && input.getProperties().getCreated()) {
                    joinProperties.setCreated(timestamp);
                } else {
                    if (input.getProperties().getCreatedBy().trim().length() > 0 && input.getProperties().getCreatedAt() != 0) {
                        final TimeStamp timestampEdit = new TimeStamp();
                        try {
                            timestampEdit.setActor(Urn.createFromString(input.getProperties().getCreatedBy()));
                        } catch (URISyntaxException e) {
                            throw new RuntimeException(e);
                        }
                        timestampEdit.setTime(input.getProperties().getCreatedAt());
                        joinProperties.setCreated(timestampEdit);
                    }
                }
                joinProperties.setLastModified(timestamp);
                proposals.add(updateMappingHelper.aspectToProposal(joinProperties, JOIN_PROPERTIES_ASPECT_NAME));
            }
        }
        return null;
    }
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
                    throw new RuntimeException(String.format("Failed to create join entity"), e);
                }
                return true;
            } catch (Exception e) {
                log.error("Failed to create Join to resource with input {}, {}", input, e.getMessage());
                throw new RuntimeException(String.format("Failed to create join to resource with input %s", input), e);
            }
        });
    }
}
