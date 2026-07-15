package com.linkedin.datahub.upgrade.usageevents;

import com.linkedin.metadata.datahubusage.UsageEventsInfrastructureProvisioner;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Provisions partitioned PostgreSQL storage for usage events (parent table, indexes, and initial
 * partition coverage).
 */
@RequiredArgsConstructor
@Slf4j
public class PostgresUsageEventsInfrastructureProvisioner
    implements UsageEventsInfrastructureProvisioner {

  private final PostgresUsageEventsStore postgresUsageEventsStore;

  @Override
  public void provision(@Nonnull OperationContext opContext) throws Exception {
    log.info("Provisioning PostgreSQL usage-events storage (schema, table, partitions)");
    postgresUsageEventsStore.ensureParentTable();
    postgresUsageEventsStore.runPartitionMaintenance();
  }
}
