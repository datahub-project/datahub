package com.linkedin.datahub.graphql.resolvers.mutate;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.TagAssociationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
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
public class AddTagResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final TagAssociationInput input = bindArgument(environment.getArgument("input"), TagAssociationInput.class);
    Urn tagUrn = Urn.createFromString(input.getTagUrn());
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    if (!LabelUtils.isAuthorizedToUpdateTags(environment.getContext(), targetUrn, input.getSubResource())) {
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(() -> {
      LabelUtils.validateResourceAndLabel(
          tagUrn,
          targetUrn,
          input.getSubResource(),
          input.getSubResourceType(),
          Constants.TAG_ENTITY_NAME,
          _entityService,
          false
      );
      try {

        if (!tagUrn.getEntityType().equals("tag")) {
          log.error("Failed to add {}. It is not a tag urn.", tagUrn.toString());
          return false;
        }

        log.info("Adding Tag. input: {}", input.toString());
        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActorUrn());
        LabelUtils.addTagsToResources(
            ImmutableList.of(tagUrn),
            ImmutableList.of(new ResourceRefInput(input.getResourceUrn(), input.getSubResourceType(), input.getSubResource())),
            actor,
            _entityService
        );
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }
}
