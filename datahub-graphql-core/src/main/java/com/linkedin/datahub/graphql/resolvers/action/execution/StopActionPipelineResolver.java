package com.linkedin.datahub.graphql.resolvers.action.execution;

import com.linkedin.action.DataHubActionInfo;
import com.linkedin.action.DataHubActionState;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class StopActionPipelineResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;

  public CompletableFuture<String> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    if (!AuthorizationUtils.canManageActionPipelines(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    String actionPipelineUrnString = environment.getArgument("urn");
    if (actionPipelineUrnString == null) {
      throw new DataHubGraphQLException(
          "Action pipeline urn is required to stop action pipeline.",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    Urn actionPipelineUrn;
    try {
      actionPipelineUrn = Urn.createFromString(actionPipelineUrnString);
    } catch (URISyntaxException e) {
      throw new DataHubGraphQLException(
          String.format("Malformed urn %s provided.", actionPipelineUrnString),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    log.info("Action pipeline = {}", actionPipelineUrn);

    return _integrationsService
        .stopAction(actionPipelineUrn.toString())
        .thenCompose(
            actionStopped ->
                GraphQLConcurrencyUtils.supplyAsync(
                    () -> {
                      try {
                        Aspect rawAspect =
                            _entityClient.getLatestAspectObject(
                                context.getOperationContext(),
                                actionPipelineUrn,
                                "dataHubActionInfo",
                                false);
                        if (rawAspect == null) {
                          throw new DataHubGraphQLException(
                              String.format(
                                  "No dataHubActionInfo found for action pipeline %s",
                                  actionPipelineUrn),
                              DataHubGraphQLErrorCode.NOT_FOUND);
                        }

                        DataHubActionInfo dataHubActionInfo =
                            new DataHubActionInfo(rawAspect.data());
                        if (dataHubActionInfo.hasState()
                            && dataHubActionInfo.getState().equals(DataHubActionState.INACTIVE)) {
                          return actionPipelineUrn.toString();
                        }

                        if (!dataHubActionInfo.hasState()
                            || dataHubActionInfo.getState().equals(DataHubActionState.ACTIVE)) {
                          dataHubActionInfo.setState(DataHubActionState.INACTIVE);
                          _entityClient.ingestProposal(
                              context.getOperationContext(),
                              new MetadataChangeProposal()
                                  .setEntityType("dataHubAction")
                                  .setChangeType(ChangeType.UPSERT)
                                  .setAspect(GenericRecordUtils.serializeAspect(dataHubActionInfo))
                                  .setAspectName("dataHubActionInfo")
                                  .setEntityUrn(actionPipelineUrn),
                              false);
                        }

                        if (!actionStopped) {
                          throw new DataHubGraphQLException(
                              String.format("Failed to stop action pipeline %s", actionPipelineUrn),
                              DataHubGraphQLErrorCode.SERVER_ERROR);
                        }

                        return actionPipelineUrn.toString();
                      } catch (RemoteInvocationException | URISyntaxException e) {
                        throw new RuntimeException(e);
                      } catch (Exception e) {
                        log.error("Failed to stop action pipeline", e);
                        throw new RuntimeException(
                            String.format("Failed to stop action pipeline %s", actionPipelineUrn),
                            e);
                      }
                    },
                    this.getClass().getSimpleName(),
                    "get"));
  }
}
