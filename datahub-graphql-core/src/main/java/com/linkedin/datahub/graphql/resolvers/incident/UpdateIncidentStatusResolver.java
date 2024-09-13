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
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateIncidentStatusInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
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
public class UpdateIncidentStatusResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn incidentUrn = Urn.createFromString(environment.getArgument("urn"));
    final UpdateIncidentStatusInput input =
        bindArgument(environment.getArgument("input"), UpdateIncidentStatusInput.class);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {

          // Check whether the incident exists.
          IncidentInfo info =
              (IncidentInfo)
                  EntityUtils.getAspectFromEntity(
                      context.getOperationContext(),
                      incidentUrn.toString(),
                      INCIDENT_INFO_ASPECT_NAME,
                      _entityService,
                      null);

          if (info != null) {
            // Check whether the actor has permission to edit the incident
            // Currently only supporting a single entity. TODO: Support multiple incident entities.
            final Urn resourceUrn = info.getEntities().get(0);
            if (isAuthorizedToUpdateIncident(resourceUrn, context)) {
              info.setStatus(
                  new IncidentStatus()
                      .setState(IncidentState.valueOf(input.getState().name()))
                      .setLastUpdated(
                          new AuditStamp()
                              .setActor(UrnUtils.getUrn(context.getActorUrn()))
                              .setTime(System.currentTimeMillis())));
              if (input.getMessage() != null) {
                info.getStatus().setMessage(input.getMessage());
              }
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
        },
        this.getClass().getSimpleName(),
        "get");
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
