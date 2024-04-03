package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchAddTagsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.SiblingsUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchAddTagsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchAddTagsInput input =
        bindArgument(environment.getArgument("input"), BatchAddTagsInput.class);
    final List<Urn> tagUrns =
        input.getTagUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    final List<ResourceRefInput> resources = input.getResources();

    return CompletableFuture.supplyAsync(
        () -> {

          // First, validate the batch
          validateTags(tagUrns);

          if (resources.size() == 1 && resources.get(0).getSubResource() != null) {
            return handleAddTagsToSingleSchemaField(context, resources, tagUrns);
          }

          validateInputResources(resources, context);

          try {
            // Then execute the bulk add
            batchAddTags(tagUrns, resources, context);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        });
  }

  /**
   * When adding tags to a schema field in the UI, there's a chance the parent entity has siblings.
   * If the given urn doesn't have a schema or doesn't have the given column, we should try to add
   * the tag to one of its siblings. If that fails, keep trying all siblings until one passes or all
   * fail. Then we throw if none succeed.
   */
  private Boolean handleAddTagsToSingleSchemaField(
      @Nonnull final QueryContext context,
      @Nonnull final List<ResourceRefInput> resources,
      @Nonnull final List<Urn> tagUrns) {
    final ResourceRefInput resource = resources.get(0);
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
    final List<Urn> siblingUrns = SiblingsUtils.getSiblingUrns(resourceUrn, _entityService);
    return attemptBatchAddTagsWithSiblings(
        tagUrns, resource, context, new HashSet<>(), siblingUrns);
  }

  /**
   * Attempts to add tags to a schema field, and if it fails, try adding to one of its siblings. Try
   * adding until we attempt all siblings or one passes. Throw if none pass.
   */
  private Boolean attemptBatchAddTagsWithSiblings(
      @Nonnull final List<Urn> tagUrns,
      @Nonnull final ResourceRefInput resource,
      @Nonnull final QueryContext context,
      @Nonnull final HashSet<Urn> attemptedUrns,
      @Nonnull final List<Urn> siblingUrns) {
    attemptedUrns.add(UrnUtils.getUrn(resource.getResourceUrn()));
    final List<ResourceRefInput> resources = new ArrayList<>();
    resources.add(resource);

    try {
      validateInputResources(resources, context);
      batchAddTags(tagUrns, resources, context);
      return true;
    } catch (Exception e) {
      final Optional<Urn> siblingUrn = SiblingsUtils.getNextSiblingUrn(siblingUrns, attemptedUrns);

      if (siblingUrn.isPresent()) {
        log.warn(
            "Failed to add tags for resourceUrn {} and subResource {}, trying sibling urn {} now.",
            resource.getResourceUrn(),
            resource.getSubResource(),
            siblingUrn.get());
        resource.setResourceUrn(siblingUrn.get().toString());
        return attemptBatchAddTagsWithSiblings(
            tagUrns, resource, context, attemptedUrns, siblingUrns);
      } else {
        log.error(
            "Failed to perform update against resource {}, {}",
            resource.toString(),
            e.getMessage());
        throw new RuntimeException(
            String.format("Failed to perform update against resource %s", resource.toString()), e);
      }
    }
  }

  private void validateTags(List<Urn> tagUrns) {
    for (Urn tagUrn : tagUrns) {
      LabelUtils.validateLabel(tagUrn, Constants.TAG_ENTITY_NAME, _entityService);
    }
  }

  private void validateInputResources(List<ResourceRefInput> resources, QueryContext context) {
    for (ResourceRefInput resource : resources) {
      validateInputResource(resource, context);
    }
  }

  private void validateInputResource(ResourceRefInput resource, QueryContext context) {
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
    if (!LabelUtils.isAuthorizedToUpdateTags(context, resourceUrn, resource.getSubResource())) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    LabelUtils.validateResource(
        resourceUrn, resource.getSubResource(), resource.getSubResourceType(), _entityService);
  }

  private void batchAddTags(
      List<Urn> tagUrns, List<ResourceRefInput> resources, QueryContext context) {
    log.debug("Batch adding Tags. tags: {}, resources: {}", resources, tagUrns);
    try {
      LabelUtils.addTagsToResources(
          tagUrns, resources, UrnUtils.getUrn(context.getActorUrn()), _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Tags %s to resources with urns %s!",
              tagUrns,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }
}
