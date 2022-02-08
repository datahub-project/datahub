package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class UpdateDescriptionResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final DescriptionUpdateInput input = bindArgument(environment.getArgument("input"), DescriptionUpdateInput.class);
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    log.info("Updating description. input: {}", input.toString());
    switch (targetUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return updateDatasetDescription(targetUrn, input, environment.getContext());
      case Constants.CONTAINER_ENTITY_NAME:
        return updateContainerDescription(targetUrn, input, environment.getContext());
      case Constants.DOMAIN_ENTITY_NAME:
        return updateDomainDescription(targetUrn, input, environment.getContext());
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return updateGlossaryTermDescription(targetUrn, input, environment.getContext());
      case Constants.TAG_ENTITY_NAME:
        return updateTagDescription(targetUrn, input, environment.getContext());
      default:
        throw new RuntimeException(
            String.format("Failed to update description. Unsupported resource type %s provided.", targetUrn));
    }
  }

  private CompletableFuture<Boolean> updateContainerDescription(Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return CompletableFuture.supplyAsync(() -> {

      if (!DescriptionUtils.isAuthorizedToUpdateContainerDescription(context, targetUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      DescriptionUtils.validateContainerInput(targetUrn, _entityService);

      try {
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        DescriptionUtils.updateContainerDescription(
            input.getDescription(),
            targetUrn,
            actor,
            _entityService);
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }

  private CompletableFuture<Boolean> updateDomainDescription(Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return CompletableFuture.supplyAsync(() -> {

      if (!DescriptionUtils.isAuthorizedToUpdateDomainDescription(context, targetUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
        DescriptionUtils.validateDomainInput(targetUrn, _entityService);

        try {
          Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
          DescriptionUtils.updateDomainDescription(
              input.getDescription(),
              targetUrn,
              actor,
              _entityService);
          return true;
        } catch (Exception e) {
          log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
          throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
        }
    });
  }

  private CompletableFuture<Boolean> updateDatasetDescription(Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {

    return CompletableFuture.supplyAsync(() -> {

      if (!DescriptionUtils.isAuthorizedToUpdateFieldDescription(context, targetUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      if (input.getSubResourceType() == null) {
        throw new IllegalArgumentException("Update description without subresource is not currently supported");
      }

      DescriptionUtils.validateFieldDescriptionInput(targetUrn, input.getSubResource(), input.getSubResourceType(),
          _entityService);

      try {
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        DescriptionUtils.updateFieldDescription(input.getDescription(), targetUrn, input.getSubResource(), actor,
            _entityService);
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }

  private CompletableFuture<Boolean> updateTagDescription(Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return CompletableFuture.supplyAsync(() -> {

      if (!DescriptionUtils.isAuthorizedToUpdateTagDescription(context, targetUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
      DescriptionUtils.validateTagInput(targetUrn, _entityService);

      try {
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        DescriptionUtils.updateTagDescription(
            input.getDescription(),
            targetUrn,
            actor,
            _entityService);
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }

  private CompletableFuture<Boolean> updateGlossaryTermDescription(Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return CompletableFuture.supplyAsync(() -> {

      if (!DescriptionUtils.isAuthorizedToUpdateGlossaryTermDescription(context, targetUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
      DescriptionUtils.validateGlossaryTermInput(targetUrn, _entityService);

      try {
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        DescriptionUtils.updateGlossaryTermDescription(
            input.getDescription(),
            targetUrn,
            actor,
            _entityService);
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }
}
