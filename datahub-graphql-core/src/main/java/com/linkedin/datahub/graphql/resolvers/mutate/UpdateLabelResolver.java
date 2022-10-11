package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.LabelUpdateInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

@Slf4j
@RequiredArgsConstructor
public class UpdateLabelResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final LabelUpdateInput input = bindArgument(environment.getArgument("input"), LabelUpdateInput.class);
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    log.info("Updating label. input: {}", input.toString());
    log.info("Target URN: {}", targetUrn.toString());
    switch (targetUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return updateDatasetLabel(targetUrn, input, environment.getContext());
      default:
        throw new RuntimeException(
            String.format("Failed to update label. Unsupported resource type %s provided.", targetUrn));
    }
  }

  private CompletableFuture<Boolean> updateDatasetLabel(Urn targetUrn, LabelUpdateInput input, QueryContext context) {
    return CompletableFuture.supplyAsync(() -> {
      
      if (!DescriptionUtils.isAuthorizedToUpdateFieldDescription(context, targetUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      if (input.getSubResourceType() == null) {
        throw new IllegalArgumentException("Update label without subresource is not currently supported");
      }

      validateFieldLabelInput(targetUrn, input.getSubResource(), input.getSubResourceType(), _entityService);

      try {
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        log.info("UpdateFieldLabel: {}", input.getLabel());
        LabelUtils.updateFieldLabel(input.getLabel(), targetUrn, input.getSubResource(), actor,
            _entityService);
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }

  private static Boolean validateFieldLabelInput(
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService entityService
  ) {
    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    validateSubresourceExists(resourceUrn, subResource, subResourceType, entityService);

    return true;
  }

}
