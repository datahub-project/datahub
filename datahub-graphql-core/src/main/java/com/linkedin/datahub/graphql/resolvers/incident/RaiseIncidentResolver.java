package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.incident.IncidentUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.RaiseIncidentInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.key.IncidentKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
            if (!IncidentUtils.isAuthorizedToEditIncidentForResource(urn, context)) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }
          }

          // Presence opts into client-owned identity. Reject blank ids instead of treating them as
          // omitted, because retrying a blank id must not create a new random incident each time.
          final boolean callerProvidedId = input.getId() != null;
          if (callerProvidedId && input.getId().isBlank()) {
            throw new DataHubGraphQLException(
                "Incident id must not be blank.", DataHubGraphQLErrorCode.BAD_REQUEST);
          }
          final String id = callerProvidedId ? input.getId() : UUID.randomUUID().toString();

          try {
            final IncidentKey key = new IncidentKey();
            key.setId(id);

            final IncidentInfo incidentInfo = mapIncidentInfo(input, resourceUrns, context);
            final MetadataChangeProposal proposal =
                callerProvidedId
                    ? buildCreateIfNotExistsProposal(key, incidentInfo)
                    : buildMetadataChangeProposalWithKey(
                        key, INCIDENT_ENTITY_NAME, INCIDENT_INFO_ASPECT_NAME, incidentInfo);

            final String resultUrn =
                _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            if (callerProvidedId && resultUrn == null) {
              // CreateIfNotExistsValidator filtered the CREATE_ENTITY write because an Incident
              // already exists at this id. Surface that as a conflict rather than treating the
              // filtered write as a silent success.
              //
              // This null check is only a reliable conflict signal with JavaEntityClient:
              // JavaEntityClient.batchIngestProposals omits FILTER-dropped items from its
              // returned URN list, so ingestProposal returns null here. RestliEntityClient does
              // not behave the same way -- on Rest.li SUCCESS it derives the URN from the MCP
              // itself and returns it even when the write was filtered, which would make this
              // look like a get-or-create instead of a conflict. Default GMS GraphQL wires
              // JavaEntityClient, so this holds today; do not remove this check as unreachable,
              // and if GraphQL is ever wired through Restli, this conflict contract breaks.
              throw new DataHubGraphQLException(
                  String.format("Incident with id %s already exists.", id),
                  DataHubGraphQLErrorCode.CONFLICT);
            }
            return resultUrn;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            log.error("Failed to create incident. {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Builds a CREATE_ENTITY proposal carrying the If-None-Match: * precondition header, so {@link
   * CreateIfNotExistsValidator} filters (rather than applies) the write when an Incident already
   * exists at this key. {@link #get} treats a filtered write, seen as a null urn back from
   * ingestProposal, as a conflict rather than a silent success.
   */
  private MetadataChangeProposal buildCreateIfNotExistsProposal(
      final IncidentKey key, final IncidentInfo incidentInfo) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(INCIDENT_ENTITY_NAME);
    proposal.setAspectName(INCIDENT_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(incidentInfo));
    proposal.setChangeType(ChangeType.CREATE_ENTITY);
    proposal.setHeaders(
        new StringMap(
            Map.of(
                CreateIfNotExistsValidator.FILTER_EXCEPTION_HEADER,
                CreateIfNotExistsValidator.FILTER_EXCEPTION_VALUE)));
    return applyProposalUiSource(proposal);
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
        && (input.getCustomType() == null || input.getCustomType().isBlank())) {
      throw new IllegalArgumentException(
          "Failed to raise incident: customType is required when type is CUSTOM");
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
}
