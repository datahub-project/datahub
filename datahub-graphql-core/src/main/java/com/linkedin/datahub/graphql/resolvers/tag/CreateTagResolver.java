package com.linkedin.datahub.graphql.resolvers.tag;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateTagInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.TagKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.tag.TagProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for creating a new Tag on DataHub. Requires the CREATE_TAG or MANAGE_TAGS
 * privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class CreateTagResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateTagInput input =
        bindArgument(environment.getArgument("input"), CreateTagInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canCreateTags(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            // Create the Tag Key
            final TagKey key = new TagKey();

            // Take user provided id OR generate a random UUID for the Tag.
            final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            key.setName(id);

            if (_entityClient.exists(
                EntityKeyUtils.convertEntityKeyToUrn(key, TAG_ENTITY_NAME),
                context.getAuthentication())) {
              throw new IllegalArgumentException("This Tag already exists!");
            }

            // Create the MCP
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key, TAG_ENTITY_NAME, TAG_PROPERTIES_ASPECT_NAME, mapTagProperties(input));
            String tagUrn =
                _entityClient.ingestProposal(proposal, context.getAuthentication(), false);
            OwnershipType ownershipType = OwnershipType.TECHNICAL_OWNER;
            if (!_entityService.exists(
                UrnUtils.getUrn(mapOwnershipTypeToEntity(ownershipType.name())))) {
              log.warn("Technical owner does not exist, defaulting to None ownership.");
              ownershipType = OwnershipType.NONE;
            }

            OwnerUtils.addCreatorAsOwner(
                context, tagUrn, OwnerEntityType.CORP_USER, ownershipType, _entityService);
            return tagUrn;
          } catch (Exception e) {
            log.error(
                "Failed to create Tag with id: {}, name: {}: {}",
                input.getId(),
                input.getName(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to create Tag with id: %s, name: %s", input.getId(), input.getName()),
                e);
          }
        });
  }

  private TagProperties mapTagProperties(final CreateTagInput input) {
    final TagProperties result = new TagProperties();
    result.setName(input.getName());
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    return result;
  }
}
