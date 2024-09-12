package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.ALL_PRIVILEGES_GROUP;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateIncidentInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

/** GraphQL Resolver that updates an incident's status */
@RequiredArgsConstructor
public class UpdateIncidentResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn incidentUrn = Urn.createFromString(environment.getArgument("urn"));
    final UpdateIncidentInput input =
        bindArgument(environment.getArgument("input"), UpdateIncidentInput.class);
    return CompletableFuture.supplyAsync(
        () -> {

          // Check whether the incident exists.
          final IncidentInfo info =
              (IncidentInfo)
                  EntityUtils.getAspectFromEntity(
                      context.getOperationContext(),
                      incidentUrn.toString(),
                      INCIDENT_INFO_ASPECT_NAME,
                      _entityService,
                      null);

          if (info != null) {
            // Check whether the actor has permission to edit the incident.
            // Currently, this depends on the single entity that the incident is associated with.
            final Urn resourceUrn = info.getEntities().get(0);
            if (isAuthorizedToUpdateIncident(resourceUrn, context)) {
              final AuditStamp actorStamp =
                  new AuditStamp()
                      .setActor(UrnUtils.getUrn(context.getActorUrn()))
                      .setTime(System.currentTimeMillis());
              updateIncidentInfo(info, input, actorStamp);
              try {
                // Finally, create the MetadataChangeProposal.
                final MetadataChangeProposal proposal =
                    buildMetadataChangeProposalWithUrn(
                        incidentUrn, INCIDENT_INFO_ASPECT_NAME, info);
                _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
                return true;
              } catch (Exception e) {
                throw new RuntimeException("Failed to update incident status!", e);
              }
            }
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          throw new DataHubGraphQLException(
              "Failed to update incident. Incident does not exist.",
              DataHubGraphQLErrorCode.NOT_FOUND);
        });
  }

  private void updateIncidentInfo(
      final IncidentInfo info, final UpdateIncidentInput input, final AuditStamp actorStamp) {
    if (input.getTitle() != null) {
      info.setTitle(input.getTitle());
    }
    if (input.getDescription() != null) {
      info.setDescription(input.getDescription());
    }
    if (input.getPriority() != null) {
      info.setPriority(IncidentUtils.mapIncidentPriority(input.getPriority()));
    }
    if (input.getAssigneeUrns() != null) {
      info.setAssignees(IncidentUtils.mapIncidentAssignees(input.getAssigneeUrns(), actorStamp));
    }
    if (input.getStatus() != null) {
      info.setStatus(IncidentUtils.mapIncidentStatus(input.getStatus(), actorStamp));
    }
  }

  private boolean isAuthorizedToUpdateIncident(final Urn resourceUrn, final QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_INCIDENTS_PRIVILEGE.getType()))));
    return AuthorizationUtils.isAuthorized(
        context, resourceUrn.getEntityType(), resourceUrn.toString(), orPrivilegeGroups);
  }
}
