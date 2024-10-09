package com.linkedin.metadata.boot.steps;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.AcrylConstants.*;

import com.linkedin.action.DataHubActionConfig;
import com.linkedin.action.DataHubActionInfo;
import com.linkedin.action.DataHubActionRequestSource;
import com.linkedin.action.DataHubActionSource;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.ForwardingActionConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class IngestionMetadataTestResultsActionStep implements BootstrapStep {

  private final EntityService<?> entityService;
  private final IntegrationsService integrationsService;
  private final ForwardingActionConfiguration config;

  public IngestionMetadataTestResultsActionStep(
      EntityService<?> entityService,
      IntegrationsService integrationsService,
      ForwardingActionConfiguration forwardingActionConfiguration) {
    this.entityService = entityService;
    this.integrationsService = integrationsService;
    this.config = forwardingActionConfiguration;
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute(@NotNull OperationContext opContext) throws Exception {
    try {
      log.info("Action pipeline config = {}", config.getRecipe());
      DataHubActionInfo actionInfo = new DataHubActionInfo();
      actionInfo.setType(METADATA_TESTS_FORWARDING_ACTION_TYPE);
      actionInfo.setName(METADATA_TESTS_FORWARDING_ACTION_DISPLAY_NAME);
      actionInfo.setConfig(new DataHubActionConfig().setRecipe(config.getRecipe()));
      actionInfo.setSource(new DataHubActionSource().setType(DataHubActionRequestSource.SYSTEM));
      log.info("Action Info aspect = {}", actionInfo);

      final MetadataChangeProposal proposal =
          buildMetadataChangeProposalWithUrn(
              UrnUtils.getUrn(FORWARDING_ACTION_URN),
              Constants.ACTIONS_PIPELINE_INFO_ASPECT_NAME,
              actionInfo);

      AspectsBatch aspectsBatch =
          AspectsBatchImpl.builder()
              .mcps(
                  Collections.singletonList(proposal),
                  opContext.getAuditStamp(),
                  opContext.getRetrieverContext().get())
              .build();

      entityService.ingestProposal(opContext, aspectsBatch, false);

      // We force blocking here as this is a part of bootstrap and blocking does not interfere with
      // usability
      if (!integrationsService.reloadAction(FORWARDING_ACTION_URN).get()) {
        log.error("Failed to reload action pipeline {}", FORWARDING_ACTION_URN);
      }
    } catch (Exception e) {
      log.error("Failed to ingest action pipeline", e);
    }
  }

  @NotNull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }
}
