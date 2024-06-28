package com.linkedin.datahub.graphql.types.ermodelrelationship;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.ERModelRelationshipUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ERModelRelationship;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipPropertiesInput;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipUpdateInput;
import com.linkedin.datahub.graphql.types.ermodelrelationship.mappers.ERModelRelationMapper;
import com.linkedin.datahub.graphql.types.ermodelrelationship.mappers.ERModelRelationshipUpdateInputMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ERModelRelationshipService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;

@Slf4j
@RequiredArgsConstructor
public class CreateERModelRelationshipResolver
    implements DataFetcher<CompletableFuture<ERModelRelationship>> {

  private final EntityClient _entityClient;
  private final ERModelRelationshipService _erModelRelationshipService;

  @Override
  public CompletableFuture<ERModelRelationship> get(DataFetchingEnvironment environment)
      throws Exception {
    final ERModelRelationshipUpdateInput input =
        bindArgument(environment.getArgument("input"), ERModelRelationshipUpdateInput.class);

    final ERModelRelationshipPropertiesInput erModelRelationshipPropertiesInput =
        input.getProperties();
    String ermodelrelationName = erModelRelationshipPropertiesInput.getName();
    String source = erModelRelationshipPropertiesInput.getSource();
    String destination = erModelRelationshipPropertiesInput.getDestination();

    String lowDataset = source;
    String highDataset = destination;
    if (source.compareTo(destination) > 0) {
      lowDataset = destination;
      highDataset = source;
    }
    // The following sequence mimics datahub.emitter.mce_builder.datahub_guid
    // Keys have to be in alphabetical order - Destination, ERModelRelationName and Source

    String ermodelrelationKey =
        "{\"Destination\":\""
            + lowDataset
            + "\",\"ERModelRelationName\":\""
            + ermodelrelationName
            + "\",\"Source\":\""
            + highDataset
            + "\"}";

    byte[] mybytes = ermodelrelationKey.getBytes(StandardCharsets.UTF_8);

    String ermodelrelationKeyEncoded = new String(mybytes, StandardCharsets.UTF_8);
    String ermodelrelationGuid = DigestUtils.md5Hex(ermodelrelationKeyEncoded);
    log.info(
        "ermodelrelationkey {}, ermodelrelationGuid {}",
        ermodelrelationKeyEncoded,
        ermodelrelationGuid);

    ERModelRelationshipUrn inputUrn = new ERModelRelationshipUrn(ermodelrelationGuid);
    QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActorUrn());
    if (!ERModelRelationshipType.canCreateERModelRelation(
        context,
        Urn.createFromString(input.getProperties().getSource()),
        Urn.createFromString(input.getProperties().getDestination()))) {
      throw new AuthorizationException(
          "Unauthorized to create erModelRelationship. Please contact your DataHub administrator.");
    }
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            log.debug("Create ERModelRelation input: {}", input);
            final Collection<MetadataChangeProposal> proposals =
                ERModelRelationshipUpdateInputMapper.map(context, input, actor);
            proposals.forEach(proposal -> proposal.setEntityUrn(inputUrn));
            try {
              _entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);
            } catch (RemoteInvocationException e) {
              throw new RuntimeException("Failed to create erModelRelationship entity", e);
            }
            return ERModelRelationMapper.map(
                context,
                _erModelRelationshipService.getERModelRelationshipResponse(
                    context.getOperationContext(), Urn.createFromString(inputUrn.toString())));
          } catch (Exception e) {
            log.error(
                "Failed to create ERModelRelation to resource with input {}, {}",
                input,
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to create erModelRelationship to resource with input %s", input),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
