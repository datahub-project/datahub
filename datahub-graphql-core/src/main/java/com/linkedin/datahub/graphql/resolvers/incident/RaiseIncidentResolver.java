package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.ALL_PRIVILEGES_GROUP;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.incident.IncidentUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RaiseIncidentInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.key.IncidentKey;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver used for creating (raising) a new asset incident. */
// TODO: Add an incident impact summary that is computed here (or in a hook)
@Slf4j
@RequiredArgsConstructor
public class RaiseIncidentResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final RaiseIncidentInput input =
        bindArgument(environment.getArgument("input"), RaiseIncidentInput.class);
    final Urn resourceUrn =
        input.getResourceUrn() != null ? Urn.createFromString(input.getResourceUrn()) : null;
    final List<Urn> resourceUrns =
        new ArrayList<>(
            input.getResourceUrns() != null
                ? stringsToUrns(input.getResourceUrns())
                : Collections.emptyList());
    if (resourceUrn != null && !resourceUrns.contains(resourceUrn)) {
      resourceUrns.add(resourceUrn);
    }
    if (resourceUrns.isEmpty()) {
      throw new RuntimeException("At least 1 resource urn must be defined to raise an incident.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          for (Urn urn : resourceUrns) {
            if (!isAuthorizedToCreateIncidentForResource(urn, context)) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }
          }

          try {
            // Create the Domain Key
            final IncidentKey key = new IncidentKey();

            // Generate a random UUID for the incident
            final String id = UUID.randomUUID().toString();
            key.setId(id);

            // Create the MCP
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key,
                    INCIDENT_ENTITY_NAME,
                    INCIDENT_INFO_ASPECT_NAME,
                    mapIncidentInfo(input, resourceUrns, context));
            return _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
          } catch (Exception e) {
            log.error("Failed to create incident. {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private IncidentInfo mapIncidentInfo(
      final RaiseIncidentInput input, List<Urn> resourceUrns, final QueryContext context)
      throws URISyntaxException {
    final AuditStamp actorStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(context.getActorUrn()))
            .setTime(System.currentTimeMillis());
    final IncidentInfo result = new IncidentInfo();
    result.setType(
        IncidentType.valueOf(
            input
                .getType()
                .name())); // Assumption Alert: This assumes that GMS incident type === GraphQL
    // incident type.
    if (IncidentType.CUSTOM.name().equals(input.getType().name())
        && input.getCustomType() == null) {
      throw new URISyntaxException("Failed to create incident.", "customType is required");
    }
    result.setCustomType(input.getCustomType(), SetMode.IGNORE_NULL);
    result.setTitle(input.getTitle(), SetMode.IGNORE_NULL);
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    result.setEntities(new UrnArray(resourceUrns));
    result.setCreated(
        new AuditStamp()
            .setActor(Urn.createFromString(context.getActorUrn()))
            .setTime(System.currentTimeMillis()));
    if (input.getStartedAt() != null) {
      result.setStartedAt(input.getStartedAt());
    }
    // Create the incident in the 'active' state by default.
    result.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL), SetMode.IGNORE_NULL);
    result.setPriority(IncidentUtils.mapIncidentPriority(input.getPriority()), SetMode.IGNORE_NULL);
    result.setAssignees(
        IncidentUtils.mapIncidentAssignees(input.getAssigneeUrns(), actorStamp),
        SetMode.IGNORE_NULL);
    result.setStatus(IncidentUtils.mapIncidentStatus(input.getStatus(), actorStamp));
    return result;
  }

  private boolean isAuthorizedToCreateIncidentForResource(
      final Urn resourceUrn, final QueryContext context) {
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
