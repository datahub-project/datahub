package com.linkedin.datahub.upgrade.system.executorpools;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AuditStampUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;

@Slf4j
public class ExecutorPools implements BlockingSystemUpgrade {

  private List<UpgradeStep> _steps;

  public ExecutorPools(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize,
      String customerId) {
    _steps = ImmutableList.of();

    if (enabled) {
      try {
        // Create SQS client
        Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
        SqsClient sqsClient = SqsClient.builder().region(region).build();
        final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
        _steps =
            ImmutableList.of(
                new ExecutorPoolsStep(
                    opContext,
                    entityService,
                    searchService,
                    sqsClient,
                    enabled,
                    reprocessEnabled,
                    batchSize,
                    customerId,
                    region,
                    auditStamp));
      } catch (Exception e) {
        log.error("Error while creating SQS client. Skipping ExecutorPools upgrade", e);
      }
    }
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
