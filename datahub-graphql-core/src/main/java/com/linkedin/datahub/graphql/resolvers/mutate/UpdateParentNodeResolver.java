package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.persistAspect;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateParentNodeInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateParentNodeResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final UpdateParentNodeInput input =
        bindArgument(environment.getArgument("input"), UpdateParentNodeInput.class);
    final QueryContext context = environment.getContext();
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    log.info("Updating parent node. input: {}", input.toString());

    if (!_entityService.exists(targetUrn)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", targetUrn, targetUrn));
    }

    GlossaryNodeUrn parentNodeUrn = null;
    if (input.getParentNode() != null) {
      parentNodeUrn = GlossaryNodeUrn.createFromString(input.getParentNode());
      if (!_entityService.exists(parentNodeUrn)
          || !parentNodeUrn.getEntityType().equals(Constants.GLOSSARY_NODE_ENTITY_NAME)) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to update %s. %s either does not exist or is not a glossaryNode.",
                targetUrn, parentNodeUrn));
      }
    }

    GlossaryNodeUrn finalParentNodeUrn = parentNodeUrn;
    return CompletableFuture.supplyAsync(
        () -> {
          Urn currentParentUrn = GlossaryUtils.getParentUrn(targetUrn, context, _entityClient);
          // need to be able to manage current parent node and new parent node
          if (GlossaryUtils.canManageChildrenEntities(context, currentParentUrn, _entityClient)
              && GlossaryUtils.canManageChildrenEntities(
                  context, finalParentNodeUrn, _entityClient)) {
            switch (targetUrn.getEntityType()) {
              case Constants.GLOSSARY_TERM_ENTITY_NAME:
                return updateGlossaryTermParentNode(
                    targetUrn, finalParentNodeUrn, input, environment.getContext());
              case Constants.GLOSSARY_NODE_ENTITY_NAME:
                return updateGlossaryNodeParentNode(
                    targetUrn, finalParentNodeUrn, input, environment.getContext());
              default:
                throw new RuntimeException(
                    String.format(
                        "Failed to update parentNode. Unsupported resource type %s provided.",
                        targetUrn));
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private Boolean updateGlossaryTermParentNode(
      Urn targetUrn,
      GlossaryNodeUrn parentNodeUrn,
      UpdateParentNodeInput input,
      QueryContext context) {
    try {
      GlossaryTermInfo glossaryTermInfo =
          (GlossaryTermInfo)
              EntityUtils.getAspectFromEntity(
                  targetUrn.toString(),
                  Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
                  _entityService,
                  null);
      if (glossaryTermInfo == null) {
        // If there is no info aspect for the term already, then we should throw since the model
        // also requires a name.
        throw new IllegalArgumentException("Info for this Glossary Term does not yet exist!");
      }

      if (parentNodeUrn != null) {
        glossaryTermInfo.setParentNode(parentNodeUrn);
      } else {
        glossaryTermInfo.removeParentNode();
      }
      Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
      persistAspect(
          targetUrn,
          Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
          glossaryTermInfo,
          actor,
          _entityService);

      return true;
    } catch (Exception e) {
      log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
      throw new RuntimeException(
          String.format("Failed to perform update against input %s", input.toString()), e);
    }
  }

  private Boolean updateGlossaryNodeParentNode(
      Urn targetUrn,
      GlossaryNodeUrn parentNodeUrn,
      UpdateParentNodeInput input,
      QueryContext context) {
    try {
      GlossaryNodeInfo glossaryNodeInfo =
          (GlossaryNodeInfo)
              EntityUtils.getAspectFromEntity(
                  targetUrn.toString(),
                  Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
                  _entityService,
                  null);
      if (glossaryNodeInfo == null) {
        throw new IllegalArgumentException("Info for this Glossary Node does not yet exist!");
      }

      if (parentNodeUrn != null) {
        glossaryNodeInfo.setParentNode(parentNodeUrn);
      } else {
        glossaryNodeInfo.removeParentNode();
      }
      Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
      persistAspect(
          targetUrn,
          Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
          glossaryNodeInfo,
          actor,
          _entityService);

      return true;
    } catch (Exception e) {
      log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
      throw new RuntimeException(
          String.format("Failed to perform update against input %s", input.toString()), e);
    }
  }
}
