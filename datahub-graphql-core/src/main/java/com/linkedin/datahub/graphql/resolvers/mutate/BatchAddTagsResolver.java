package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchAddTagsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class BatchAddTagsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchAddTagsInput input = bindArgument(environment.getArgument("input"), BatchAddTagsInput.class);
    final List<Urn> tagUrns = input.getTagUrns().stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());
    final List<ResourceRefInput> resources = input.getResources();

    return CompletableFuture.supplyAsync(() -> {

      // First, validate the batch
      validateTags(tagUrns);
      validateInputResources(resources, context);

      try {
        // Then execute the bulk add
        batchAddTags(tagUrns, resources, context);
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
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
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    LabelUtils.validateResource(resourceUrn, resource.getSubResource(), resource.getSubResourceType(), _entityService);
  }

  private void batchAddTags(List<Urn> tagUrns, List<ResourceRefInput> resources, QueryContext context) {
      log.debug("Batch adding Tags. tags: {}, resources: {}", resources, tagUrns);
      try {
        LabelUtils.addTagsToResources(tagUrns, resources, UrnUtils.getUrn(context.getActorUrn()), _entityService);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to batch add Tags %s to resources with urns %s!",
            tagUrns,
            resources.stream().map(ResourceRefInput::getResourceUrn).collect(Collectors.toList())),
          e);
      }
  }
}