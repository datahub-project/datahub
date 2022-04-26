package com.linkedin.datahub.graphql.resolvers.deprecation;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateDeprecationInput;
import com.linkedin.datahub.graphql.resolvers.AuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


/**
 * Resolver used for updating the Domain associated with a Metadata Asset. Requires the EDIT_DOMAINS privilege for a particular asset.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateDeprecationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private static final String EMPTY_STRING = "";
  private final EntityClient _entityClient;
  private final EntityService _entityService;  // TODO: Remove this when 'exists' added to EntityClient

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDeprecationInput input = bindArgument(environment.getArgument("input"), UpdateDeprecationInput.class);
    final Urn entityUrn = Urn.createFromString(input.getUrn());

    return CompletableFuture.supplyAsync(() -> {

      if (!isAuthorizedToUpdateDeprecationForEntity(environment.getContext(), entityUrn)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
      validateUpdateDeprecationInput(
          entityUrn,
          _entityService
      );
      try {
        Deprecation deprecation = (Deprecation) getAspectFromEntity(
            entityUrn.toString(),
            Constants.DEPRECATION_ASPECT_NAME,
            _entityService,
            new Deprecation());
        updateDeprecation(deprecation, input, context);

        // Create the Deprecation aspect
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(entityUrn);
        proposal.setEntityType(entityUrn.getEntityType());
        proposal.setAspectName(Constants.DEPRECATION_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(deprecation));
        proposal.setChangeType(ChangeType.UPSERT);
        _entityClient.ingestProposal(proposal, context.getAuthentication());
        return true;
      } catch (Exception e) {
        log.error("Failed to update Deprecation for resource with entity urn {}: {}", entityUrn, e.getMessage());
        throw new RuntimeException(String.format("Failed to update Deprecation for resource with entity urn %s", entityUrn), e);
      }
    });
  }

  private boolean isAuthorizedToUpdateDeprecationForEntity(final QueryContext context, final Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        AuthUtils.ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }

  public static Boolean validateUpdateDeprecationInput(
      Urn entityUrn,
      EntityService entityService
  ) {

    if (!entityService.exists(entityUrn)) {
      throw new IllegalArgumentException(
          String.format("Failed to update deprecation for Entity %s. Entity does not exist.", entityUrn));
    }

    return true;
  }

  private static void updateDeprecation(Deprecation deprecation, UpdateDeprecationInput input, QueryContext context) {
    deprecation.setDeprecated(input.getDeprecated());
    deprecation.setDecommissionTime(input.getDecommissionTime(), SetMode.REMOVE_IF_NULL);
    if (input.getNote() != null) {
      deprecation.setNote(input.getNote());
    } else {
      // Note is required field in GMS. Set to empty string if not provided.
      deprecation.setNote(EMPTY_STRING);
    }
    try {
      deprecation.setActor(Urn.createFromString(context.getActorUrn()));
    } catch (URISyntaxException e) {
      // Should never happen.
      throw new RuntimeException(
          String.format("Failed to convert authorized actor into an Urn. actor urn: %s",
          context.getActorUrn()),
          e);
    }
  }
}