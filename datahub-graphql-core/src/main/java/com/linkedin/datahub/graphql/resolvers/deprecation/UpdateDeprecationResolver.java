package com.linkedin.datahub.graphql.resolvers.deprecation;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.ALL_PRIVILEGES_GROUP;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateDeprecationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for updating the Domain associated with a Metadata Asset. Requires the EDIT_DOMAINS
 * privilege for a particular asset.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateDeprecationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private static final String EMPTY_STRING = "";
  private final EntityClient _entityClient;
  private final EntityService<?>
      _entityService; // TODO: Remove this when 'exists' added to EntityClient

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDeprecationInput input =
        bindArgument(environment.getArgument("input"), UpdateDeprecationInput.class);
    final Urn entityUrn = Urn.createFromString(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!isAuthorizedToUpdateDeprecationForEntity(context, entityUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          validateUpdateDeprecationInput(context.getOperationContext(), entityUrn, _entityService);
          try {
            Deprecation deprecation =
                (Deprecation)
                    EntityUtils.getAspectFromEntity(
                        context.getOperationContext(),
                        entityUrn.toString(),
                        DEPRECATION_ASPECT_NAME,
                        _entityService,
                        new Deprecation());
            updateDeprecation(deprecation, input, context);

            // Create the Deprecation aspect
            final MetadataChangeProposal proposal =
                MutationUtils.buildMetadataChangeProposalWithUrn(
                    entityUrn, DEPRECATION_ASPECT_NAME, deprecation);
            _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update Deprecation for resource with entity urn {}: {}",
                entityUrn,
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to update Deprecation for resource with entity urn %s", entityUrn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private boolean isAuthorizedToUpdateDeprecationForEntity(
      final QueryContext context, final Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups);
  }

  public static Boolean validateUpdateDeprecationInput(
      @Nonnull OperationContext opContext, Urn entityUrn, EntityService<?> entityService) {

    if (!entityService.exists(opContext, entityUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update deprecation for Entity %s. Entity does not exist.", entityUrn));
    }

    return true;
  }

  private static void updateDeprecation(
      Deprecation deprecation, UpdateDeprecationInput input, QueryContext context) {
    deprecation.setDeprecated(input.getDeprecated());
    deprecation.setDecommissionTime(input.getDecommissionTime(), SetMode.REMOVE_IF_NULL);
    if (input.getNote() != null) {
      deprecation.setNote(input.getNote());
    } else {
      // Note is required field in GMS. Set to empty string if not provided.
      deprecation.setNote(EMPTY_STRING);
    }

    try {
      deprecation.setReplacement(
          input.getReplacement() != null ? Urn.createFromString(input.getReplacement()) : null,
          SetMode.REMOVE_IF_NULL);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    try {
      deprecation.setActor(Urn.createFromString(context.getActorUrn()));
    } catch (URISyntaxException e) {
      // Should never happen.
      throw new RuntimeException(
          String.format(
              "Failed to convert authorized actor into an Urn. actor urn: %s",
              context.getActorUrn()),
          e);
    }
  }
}
