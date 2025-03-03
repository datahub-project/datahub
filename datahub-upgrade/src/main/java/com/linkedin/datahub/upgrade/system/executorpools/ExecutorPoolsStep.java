package com.linkedin.datahub.upgrade.system.executorpools;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYNC_INDEX_UPDATE_HEADER_NAME;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.executorpool.*;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.paginators.ListQueuesIterable;

@Slf4j
public class ExecutorPoolsStep implements UpgradeStep {

  private static final String UPGRADE_ID = ExecutorPools.class.getSimpleName();
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final boolean enabled;
  private final boolean reprocessEnabled;
  private final Integer batchSize;
  private final String customerId;

  public ExecutorPoolsStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize,
      String customerId) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.enabled = enabled;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
    this.customerId = customerId;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

      if (customerId == null || customerId.equals("unset")) {
        log.error("Customer ID is not set. Set the variable and re-run the upgrade.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      try {
        SqsClient sqsClient = SqsClient.create();
        String queueNamePrefix = "re-" + customerId;
        Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();

        ingestLegacyExecutorPools(sqsClient, queueNamePrefix, region, auditStamp);
      } catch (Exception e) {
        log.error("Error ingesting legacy executor pools", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      BootstrapStep.setUpgradeResult(systemOpContext, UPGRADE_ID_URN, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private void ingestLegacyExecutorPools(
      SqsClient sqsClient, String queueNamePrefix, Region region, AuditStamp auditStamp) {

    List<MetadataChangeProposal> mcps = new ArrayList<>();

    ListQueuesRequest listQueuesRequest =
        ListQueuesRequest.builder().queueNamePrefix(queueNamePrefix).build();
    ListQueuesIterable listQueues = sqsClient.listQueuesPaginator(listQueuesRequest);

    listQueues.stream()
        .flatMap(r -> r.queueUrls().stream())
        .forEach(
            queueUrl -> {
              String[] urlParts = queueUrl.split("/");
              String urlName = urlParts[urlParts.length - 1];
              String executorId = urlName.substring(queueNamePrefix.length() + 1);

              final Urn poolUrn =
                  Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, executorId);

              boolean poolExists =
                  entityService.exists(
                      systemOpContext,
                      poolUrn,
                      AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
                      false);
              if (poolExists) {
                log.info(
                    String.format("Executor pool %s already exists, skipping ingestion.", poolUrn));
                return;
              }

              final RemoteExecutorPoolState poolState = new RemoteExecutorPoolState();
              poolState.setStatus(RemoteExecutorPoolStatus.READY);

              final RemoteExecutorPoolInfo poolInfo = new RemoteExecutorPoolInfo();
              poolInfo.setCreatedAt(System.currentTimeMillis());
              poolInfo.setCreator(UrnUtils.getUrn(Constants.SYSTEM_ACTOR));
              poolInfo.setDescription("Created by upgrade job");
              poolInfo.setQueueUrl(queueUrl);
              poolInfo.setQueueRegion(region.toString());
              poolInfo.setIsEmbedded(false);
              poolInfo.setState(poolState);

              final MetadataChangeProposal proposal = new MetadataChangeProposal();
              proposal.setEntityUrn(poolUrn);
              proposal.setEntityType(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME);
              proposal.setAspectName(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);
              proposal.setChangeType(ChangeType.UPSERT);
              proposal.setAspect(GenericRecordUtils.serializeAspect(poolInfo));
              proposal.setHeaders(
                  new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))));
              proposal.setSystemMetadata(createDefaultSystemMetadata());

              mcps.add(proposal);
            });

    log.debug(String.format("Ingestion legacy executor pools for %s urns", mcps.size()));
    AspectsBatch batch =
        AspectsBatchImpl.builder()
            .mcps(mcps, auditStamp, systemOpContext.getRetrieverContext())
            .build();
    entityService.ingestProposal(systemOpContext, batch, false);
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variables to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled && enabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            systemOpContext, UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return (previouslyRun || !enabled);
  }
}
