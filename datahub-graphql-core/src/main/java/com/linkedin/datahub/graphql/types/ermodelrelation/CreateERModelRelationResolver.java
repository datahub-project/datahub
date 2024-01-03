package com.linkedin.datahub.graphql.types.ermodelrelation;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.ERModelRelationUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ERModelRelationPropertiesInput;
import com.linkedin.datahub.graphql.generated.ERModelRelationUpdateInput;
import com.linkedin.datahub.graphql.types.ermodelrelation.mappers.ERModelRelationMapper;
import com.linkedin.datahub.graphql.types.ermodelrelation.mappers.ERModelRelationUpdateInputMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ERModelRelationService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.linkedin.datahub.graphql.generated.ERModelRelation;
import org.apache.commons.codec.digest.DigestUtils;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


@Slf4j
@RequiredArgsConstructor
public class CreateERModelRelationResolver implements DataFetcher<CompletableFuture<ERModelRelation>> {

    private final EntityClient _entityClient;
    private final ERModelRelationService _ermodelrelationService;

    @Override
    public CompletableFuture<ERModelRelation> get(DataFetchingEnvironment environment) throws Exception {
        final ERModelRelationUpdateInput input = bindArgument(environment.getArgument("input"), ERModelRelationUpdateInput.class);

        final ERModelRelationPropertiesInput ermodelrelationPropertiesInput = input.getProperties();
        String ermodelrelationName = ermodelrelationPropertiesInput.getName();
        String datasetA = ermodelrelationPropertiesInput.getDataSetA();
        String datasetB = ermodelrelationPropertiesInput.getDatasetB();

        String lowDataset =  datasetA;
        String highDataset = datasetB;
        if (datasetA.compareTo(datasetB)  > 0) {
            lowDataset = datasetB;
            highDataset = datasetA;
        }
        // The following sequence mimics datahub.emitter.mce_builder.datahub_guid

        String ermodelrelationKey =
            "{\"DatasetA\":\"" + lowDataset + "\",\"DatasetB\":\"" + highDataset + "\",\"ERModelRelationName\":\"" + ermodelrelationName
                + "\"}";

        byte[] mybytes = ermodelrelationKey.getBytes(StandardCharsets.UTF_8);

        String ermodelrelationKeyEncoded = new String(mybytes, StandardCharsets.UTF_8);
        String ermodelrelationGuid = DigestUtils.md5Hex(ermodelrelationKeyEncoded);
        log.info("ermodelrelationkey {}, ermodelrelationGuid {}", ermodelrelationKeyEncoded, ermodelrelationGuid);

        ERModelRelationUrn inputUrn = new ERModelRelationUrn(ermodelrelationGuid);
        QueryContext context = environment.getContext();
        final Authentication authentication = context.getAuthentication();
        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActorUrn());
        if (!ERModelRelationType.canCreateERModelRelation(context, Urn.createFromString(input.getProperties().getDataSetA()),
                Urn.createFromString(input.getProperties().getDatasetB()))) {
            throw new AuthorizationException("Unauthorized to create ermodelrelation. Please contact your DataHub administrator.");
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("Create ERModelRelation input: {}", input);
                final Collection<MetadataChangeProposal> proposals = ERModelRelationUpdateInputMapper.map(input, actor);
                proposals.forEach(proposal -> proposal.setEntityUrn(inputUrn));
                try {
                    _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
                } catch (RemoteInvocationException e) {
                    throw new RuntimeException("Failed to create ermodelrelation entity", e);
                }
                return ERModelRelationMapper.map(_ermodelrelationService.getERModelRelationResponse(Urn.createFromString(inputUrn.toString()), authentication));
            } catch (Exception e) {
                log.error("Failed to create ERModelRelation to resource with input {}, {}", input, e.getMessage());
                throw new RuntimeException(String.format("Failed to create ermodelrelation to resource with input %s", input), e);
            }
        });
    }
}
