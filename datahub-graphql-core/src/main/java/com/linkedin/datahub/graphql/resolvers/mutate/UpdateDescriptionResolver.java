package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.SiblingsUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateDescriptionResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final DescriptionUpdateInput input =
        bindArgument(environment.getArgument("input"), DescriptionUpdateInput.class);
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    log.info("Updating description. input: {}", input.toString());
    switch (targetUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return updateDatasetSchemaFieldDescription(targetUrn, input, environment.getContext());
      case Constants.CONTAINER_ENTITY_NAME:
        return updateContainerDescription(targetUrn, input, environment.getContext());
      case Constants.DOMAIN_ENTITY_NAME:
        return updateDomainDescription(targetUrn, input, environment.getContext());
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return updateGlossaryTermDescription(targetUrn, input, environment.getContext());
      case Constants.GLOSSARY_NODE_ENTITY_NAME:
        return updateGlossaryNodeDescription(targetUrn, input, environment.getContext());
      case Constants.TAG_ENTITY_NAME:
        return updateTagDescription(targetUrn, input, environment.getContext());
      case Constants.CORP_GROUP_ENTITY_NAME:
        return updateCorpGroupDescription(targetUrn, input, environment.getContext());
      case Constants.NOTEBOOK_ENTITY_NAME:
        return updateNotebookDescription(targetUrn, input, environment.getContext());
      case Constants.ML_MODEL_ENTITY_NAME:
        return updateMlModelDescription(targetUrn, input, environment.getContext());
      case Constants.ML_MODEL_GROUP_ENTITY_NAME:
        return updateMlModelGroupDescription(targetUrn, input, environment.getContext());
      case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
        return updateMlFeatureTableDescription(targetUrn, input, environment.getContext());
      case Constants.ML_FEATURE_ENTITY_NAME:
        return updateMlFeatureDescription(targetUrn, input, environment.getContext());
      case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
        return updateMlPrimaryKeyDescription(targetUrn, input, environment.getContext());
      case Constants.DATA_PRODUCT_ENTITY_NAME:
        return updateDataProductDescription(targetUrn, input, environment.getContext());
      case Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME:
        return updateBusinessAttributeDescription(targetUrn, input, environment.getContext());
      default:
        throw new RuntimeException(
            String.format(
                "Failed to update description. Unsupported resource type %s provided.", targetUrn));
    }
  }

  private CompletableFuture<Boolean> updateContainerDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateContainerDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          DescriptionUtils.validateContainerInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateContainerDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateContainerDescription");
  }

  private CompletableFuture<Boolean> updateDomainDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDomainDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateDomainInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateDomainDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateDomainDescription");
  }

  // If updating schema field description fails, try again on a sibling until there are no more
  // siblings to try. Then throw if necessary.
  private Boolean attemptUpdateDatasetSchemaFieldDescription(
      @Nonnull final Urn targetUrn,
      @Nonnull final DescriptionUpdateInput input,
      @Nonnull final QueryContext context,
      @Nonnull final HashSet<Urn> attemptedUrns,
      @Nonnull final List<Urn> siblingUrns) {
    attemptedUrns.add(targetUrn);
    try {
      DescriptionUtils.validateFieldDescriptionInput(
          context.getOperationContext(),
          targetUrn,
          input.getSubResource(),
          input.getSubResourceType(),
          _entityService);

      final Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
      DescriptionUtils.updateFieldDescription(
          context.getOperationContext(),
          input.getDescription(),
          targetUrn,
          input.getSubResource(),
          actor,
          _entityService);
      return true;
    } catch (Exception e) {
      final Optional<Urn> siblingUrn = SiblingsUtils.getNextSiblingUrn(siblingUrns, attemptedUrns);

      if (siblingUrn.isPresent()) {
        log.warn(
            "Failed to update description for input {}, trying sibling urn {} now.",
            input.toString(),
            siblingUrn.get());
        return attemptUpdateDatasetSchemaFieldDescription(
            siblingUrn.get(), input, context, attemptedUrns, siblingUrns);
      } else {
        log.error(
            "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(
            String.format("Failed to perform update against input %s", input.toString()), e);
      }
    }
  }

  private CompletableFuture<Boolean> updateDatasetSchemaFieldDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateFieldDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          if (input.getSubResourceType() == null) {
            throw new IllegalArgumentException(
                "Update description without subresource is not currently supported");
          }

          List<Urn> siblingUrns =
              SiblingsUtils.getSiblingUrns(
                  context.getOperationContext(), targetUrn, _entityService);

          return attemptUpdateDatasetSchemaFieldDescription(
              targetUrn, input, context, new HashSet<>(), siblingUrns);
        },
        this.getClass().getSimpleName(),
        "updateDatasetSchemaFieldDescription");
  }

  private CompletableFuture<Boolean> updateTagDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateTagDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateTagDescription");
  }

  private CompletableFuture<Boolean> updateGlossaryTermDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Urn parentNodeUrn = GlossaryUtils.getParentUrn(targetUrn, context, _entityClient);
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)
              && !GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateGlossaryTermDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateGlossaryTermDescription");
  }

  private CompletableFuture<Boolean> updateGlossaryNodeDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Urn parentNodeUrn = GlossaryUtils.getParentUrn(targetUrn, context, _entityClient);
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)
              && !GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateGlossaryNodeDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateGlossaryNodeDescription");
  }

  private CompletableFuture<Boolean> updateCorpGroupDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateCorpGroupInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateCorpGroupDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateCorpGroupDescription");
  }

  private CompletableFuture<Boolean> updateNotebookDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateNotebookInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateNotebookDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateNotebookDescription");
  }

  private CompletableFuture<Boolean> updateMlModelDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateMlModelDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateMlModelDescription");
  }

  private CompletableFuture<Boolean> updateMlModelGroupDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateMlModelGroupDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateMlModelGroupDescription");
  }

  private CompletableFuture<Boolean> updateMlFeatureDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateMlFeatureDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateMlFeatureDescription");
  }

  private CompletableFuture<Boolean> updateMlPrimaryKeyDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateMlPrimaryKeyDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateMlPrimaryKeyDescription");
  }

  private CompletableFuture<Boolean> updateMlFeatureTableDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateMlFeatureTableDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateMlFeatureTableDescription");
  }

  private CompletableFuture<Boolean> updateDataProductDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateDataProductDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateDataProductDescription");
  }

  private CompletableFuture<Boolean> updateBusinessAttributeDescription(
      Urn targetUrn, DescriptionUpdateInput input, QueryContext context) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // check if user has the rights to update description for business attribute
          if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          // validate label input
          DescriptionUtils.validateLabelInput(
              context.getOperationContext(), targetUrn, _entityService);

          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            DescriptionUtils.updateBusinessAttributeDescription(
                context.getOperationContext(),
                input.getDescription(),
                targetUrn,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "updateBusinessAttributeDescription");
  }
}
