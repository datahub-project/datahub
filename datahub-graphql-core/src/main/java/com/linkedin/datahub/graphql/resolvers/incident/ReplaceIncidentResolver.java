package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ReplaceIncidentInput;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.service.IncidentInfoUpdate;
import com.linkedin.metadata.service.IncidentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

/** Replaces the fields owned by the incident editor. */
@RequiredArgsConstructor
public class ReplaceIncidentResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final IncidentService _incidentService;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn incidentUrn = Urn.createFromString(environment.getArgument("urn"));
    final ReplaceIncidentInput input =
        bindArgument(environment.getArgument("input"), ReplaceIncidentInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final IncidentInfo info =
              (IncidentInfo)
                  EntityUtils.getAspectFromEntity(
                      context.getOperationContext(),
                      incidentUrn.toString(),
                      INCIDENT_INFO_ASPECT_NAME,
                      _entityService,
                      null);
          if (info == null) {
            throw new DataHubGraphQLException(
                "Failed to replace incident. Incident does not exist.",
                DataHubGraphQLErrorCode.NOT_FOUND);
          }

          IncidentUtils.verifyAuthorizationOrThrow(context, info, input.getResourceUrns());
          AuditStamp actorStamp =
              new AuditStamp()
                  .setActor(UrnUtils.getUrn(context.getActorUrn()))
                  .setTime(System.currentTimeMillis());
          IncidentInfoUpdate replacement = IncidentUtils.mapIncidentReplacement(input, actorStamp);
          try {
            _incidentService.replaceIncident(
                context.getOperationContext(), incidentUrn, info, replacement);
            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to replace incident!", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
