package com.linkedin.datahub.graphql.resolvers.tag;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateTagInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.TagKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.tag.TagProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Resolver used for creating a new Tag on DataHub. Requires the CREATE_TAG or MANAGE_TAGS privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class CreateTagResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateTagInput input = bindArgument(environment.getArgument("input"), CreateTagInput.class);

    return CompletableFuture.supplyAsync(() -> {

      if (!AuthorizationUtils.canCreateTags(context)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      try {
        // Create the Tag Key
        final TagKey key = new TagKey();

        // Take user provided id OR generate a random UUID for the Tag.
        final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
        key.setName(id);

        if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, Constants.TAG_ENTITY_NAME), context.getAuthentication())) {
          throw new IllegalArgumentException("This Tag already exists!");
        }

        // Create the MCP
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
        proposal.setEntityType(Constants.TAG_ENTITY_NAME);
        proposal.setAspectName(Constants.TAG_PROPERTIES_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(mapTagProperties(input)));
        proposal.setChangeType(ChangeType.UPSERT);

        String tagUrn = _entityClient.ingestProposal(proposal, context.getAuthentication());
        addCreatorAsOwner(context, tagUrn);
        return tagUrn;
      } catch (Exception e) {
        log.error("Failed to create Tag with id: {}, name: {}: {}", input.getId(), input.getName(), e.getMessage());
        throw new RuntimeException(String.format("Failed to create Tag with id: %s, name: %s", input.getId(), input.getName()), e);
      }
    });
  }

  private TagProperties mapTagProperties(final CreateTagInput input) {
    final TagProperties result = new TagProperties();
    result.setName(input.getName());
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    return result;
  }

  private void addCreatorAsOwner(QueryContext context, String tagUrn) {
    try {
      Urn actorUrn = CorpuserUrn.createFromString(context.getActorUrn());
      OwnerUtils.addOwnersToResources(
          ImmutableList.of(new OwnerInput(actorUrn.toString(), OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER)),
          ImmutableList.of(new ResourceRefInput(tagUrn, null, null)),
          actorUrn,
          _entityService
      );
    } catch (Exception e) {
      log.error(String.format("Failed to add creator as owner of tag %s", tagUrn), e);
    }
  }
}