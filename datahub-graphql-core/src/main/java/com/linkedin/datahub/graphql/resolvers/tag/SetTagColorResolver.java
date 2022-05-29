package com.linkedin.datahub.graphql.resolvers.tag;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.AuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.tag.TagProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


/**
 * Resolver used for updating the Domain associated with a Metadata Asset. Requires the EDIT_DOMAINS privilege for a particular asset.
 */
@Slf4j
@RequiredArgsConstructor
public class SetTagColorResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;  // TODO: Remove this when 'exists' added to EntityClient

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final Urn tagUrn = Urn.createFromString(environment.getArgument("urn"));
    final String colorHex = environment.getArgument("colorHex");

    return CompletableFuture.supplyAsync(() -> {

      // If user is not authorized, then throw exception.
      if (!isAuthorizedToSetTagColor(environment.getContext(), tagUrn)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      // If tag does not exist, then throw exception.
      if (!_entityService.exists(tagUrn)) {
        throw new IllegalArgumentException(
            String.format("Failed to set Tag %s color. Tag does not exist.", tagUrn));
      }

      try {
        TagProperties tagProperties = (TagProperties) getAspectFromEntity(
            tagUrn.toString(),
            Constants.TAG_PROPERTIES_ASPECT_NAME,
            _entityService,
            null);

        if (tagProperties == null) {
          throw new IllegalArgumentException("Failed to set tag color. Tag properties does not yet exist!");
        }

        tagProperties.setColorHex(colorHex);

        // Update the TagProperties aspect.
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(tagUrn);
        proposal.setEntityType(tagUrn.getEntityType());
        proposal.setAspectName(Constants.TAG_PROPERTIES_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(tagProperties));
        proposal.setChangeType(ChangeType.UPSERT);
        _entityClient.ingestProposal(proposal, context.getAuthentication());
        return true;
      } catch (Exception e) {
        log.error("Failed to set color for Tag with urn {}: {}", tagUrn, e.getMessage());
        throw new RuntimeException(String.format("Failed to set color for Tag with urn %s", tagUrn), e);
      }
    });
  }

  public static boolean isAuthorizedToSetTagColor(@Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        AuthUtils.ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_TAG_COLOR_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }
}