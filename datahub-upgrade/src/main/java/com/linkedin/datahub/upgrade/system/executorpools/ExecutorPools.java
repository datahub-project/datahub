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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;

public class ExecutorPools implements BlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public ExecutorPools(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize,
      String customerId) {
    if (enabled) {
      // Create SQS client
      SqsClient sqsClient = SqsClient.create();
      Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();

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
    } else {
      _steps = ImmutableList.of();
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
