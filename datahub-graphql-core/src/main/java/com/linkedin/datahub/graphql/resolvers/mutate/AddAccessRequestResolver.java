package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.ACCESS_REQUESTS_ASPECT_NAME;

import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddAccessRequestInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AddAccessRequestResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    log.info("Running mutation addAccessRequest");
    final AddAccessRequestInput input =
        bindArgument(environment.getArgument("input"), AddAccessRequestInput.class);
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    java.util.List<StringMapEntryInput> additionalMetadataInput = input.getAdditionalMetadata();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            log.info("Adding AccessRequest. input: {}", input);
            QueryContext context = environment.getContext();
            Urn actor = Urn.createFromString(context.getAuthentication().getActor().toUrnStr());
            AuditStamp created = EntityUtils.getAuditStamp(actor);

            com.linkedin.common.AccessRequest accessRequest = new AccessRequest();
            accessRequest.setCreated(created);

            StringMap additionalMetadata = new StringMap();

            for (StringMapEntryInput element : additionalMetadataInput) {
              additionalMetadata.put(element.getKey(), element.getValue());
            }
            accessRequest.setAdditionalMetadata(additionalMetadata);
            MetadataChangeProposal proposal =
                generateAddAccessRequestEvent(targetUrn, accessRequest, _entityService);
            log.info("Sending proposal {}", proposal);
            _entityService.ingestProposal(proposal, created, false);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        });
  }

  public MetadataChangeProposal generateAddAccessRequestEvent(
      Urn targetUrn, AccessRequest accessRequest, EntityService entityService) {
    com.linkedin.common.AccessRequests currentAccessRequests =
        (com.linkedin.common.AccessRequests)
            EntityUtils.getAspectFromEntity(
                targetUrn.toString(),
                ACCESS_REQUESTS_ASPECT_NAME,
                _entityService,
                new AccessRequests());

    AccessRequestArray accessRequestArray = new AccessRequestArray();

    if (currentAccessRequests.hasRequests()) {
      accessRequestArray = currentAccessRequests.getRequests();
    }
    accessRequestArray.add(accessRequest);
    currentAccessRequests.setRequests(accessRequestArray);

    return buildMetadataChangeProposalWithUrn(
        targetUrn, ACCESS_REQUESTS_ASPECT_NAME, currentAccessRequests);
  }
}
