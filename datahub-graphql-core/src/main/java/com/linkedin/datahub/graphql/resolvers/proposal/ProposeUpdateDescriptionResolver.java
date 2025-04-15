package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.ActionRequestService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ProposeUpdateDescriptionResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final ActionRequestService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final DescriptionUpdateInput input =
        bindArgument(environment.getArgument("input"), DescriptionUpdateInput.class);
    final QueryContext context = environment.getContext();
    Urn resourceUrn = Urn.createFromString(input.getResourceUrn());
    String description = input.getDescription();
    String subResourceType =
        input.getSubResourceType() == null ? null : input.getSubResourceType().toString();
    String subresource = input.getSubResource();
    String proposalNote = input.getProposalNote();

    if (!ProposalUtils.isAuthorizedToProposeDescription(context, resourceUrn, subresource)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
    String entityType = resourceUrn.getEntityType();

    log.info("Proposing a description update. input: {}", input);
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            switch (entityType) {
              case Constants.GLOSSARY_TERM_ENTITY_NAME:
              case Constants.GLOSSARY_NODE_ENTITY_NAME:
              case Constants.DATASET_ENTITY_NAME:
              case Constants.CONTAINER_ENTITY_NAME:
              case Constants.CHART_ENTITY_NAME:
              case Constants.DASHBOARD_ENTITY_NAME:
              case Constants.DOMAIN_ENTITY_NAME:
              case Constants.ML_FEATURE_ENTITY_NAME:
              case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
              case Constants.ML_MODEL_ENTITY_NAME:
              case Constants.ML_MODEL_GROUP_ENTITY_NAME:
              case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
              case Constants.DATA_FLOW_ENTITY_NAME:
              case Constants.DATA_JOB_ENTITY_NAME:
              case Constants.DATA_PRODUCT_ENTITY_NAME:
                return _proposalService.proposeUpdateResourceDescription(
                    context.getOperationContext(),
                    actor,
                    resourceUrn,
                    subResourceType,
                    subresource,
                    description,
                    proposalNote);
              default:
                log.warn(
                    String.format(
                        "Proposing an update to a description is currently not supported for entity type %s",
                        entityType));
                return false;
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to update description", e);
          }
        });
  }
}
