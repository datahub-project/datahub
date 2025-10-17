package com.linkedin.metadata.kafka.hook.monitor;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/*
This hook is responsible for clean delete of natively evaluated assertion monitor.
 */
@Slf4j
@Component
@Import({EntityRegistryFactory.class})
public class MonitorDeletionHook implements MetadataChangeLogHook {

  private final GraphClient graphClient;
  private final EntityClient entityClient;
  private final Boolean isEnabled;

  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public MonitorDeletionHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull @Value("${monitorDeletion.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${monitorDeletion.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    entityClient = Objects.requireNonNull(systemEntityClient, "entityClient is required");
    this.graphClient = Objects.requireNonNull(graphClient, "graphClient is required");
    this.isEnabled = Objects.requireNonNull(isEnabled, "isEnabled is required");
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public MonitorDeletionHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final GraphClient graphClient,
      Boolean isEnabled) {
    this(systemEntityClient, graphClient, isEnabled, "");
  }

  @Override
  public MonitorDeletionHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public boolean isEnabled() {
    return this.isEnabled;
  }

  /**
   * Invoke the hook when a MetadataChangeLog is received
   *
   * <p>If enabled, 1. Delete Monitor if corresponding Assertion is Deleted 2. Delete Assertion if
   * corresponding Monitor is Deleted
   *
   * <p>(TBD): 3. Delete Monitor and Assertion if entity (e.g. Dataset) is deleted
   *
   * @param mcl
   */
  @Override
  public void invoke(@Nonnull MetadataChangeLog mcl) throws Exception {
    if (isEnabled) {
      log.debug("Urn {} received by Delete Monitor Hook.", mcl.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(mcl, systemOperationContext.getEntityRegistry());
      if (isAssertionHardDeletionEvent(mcl)) {
        handleAssertionHardDeleteEvent(urn);
      } else if (isMonitorHardDeletionEvent(mcl)) {
        handleMonitorHardDeleteEvent(urn);
      } else if (isDatasetHardDeletionEvent(mcl)) {
        handleDatasetHardDeleteEvent(urn);
      }
    }
  }

  private void handleMonitorHardDeleteEvent(Urn monitorUrn) {
    // find linked assertion, if any
    Urn assertionUrn = MonitorUtils.getAssertionUrnForMonitor(graphClient, monitorUrn);
    // delete assertion
    if (assertionUrn != null) {
      log.info(
          String.format(
              "Found an assertion associated with monitor being removed urn %s. Removing assertion %s",
              monitorUrn, assertionUrn));
      try {
        entityClient.deleteEntity(systemOperationContext, assertionUrn);
      } catch (RemoteInvocationException e) {
        log.error(
            String.format(
                "Caught exception while attempting to delete assertion %s associated with removed monitor urn %s! "
                    + "This means that a stale assertion may remain.",
                assertionUrn, monitorUrn),
            e);
      }
    }

    // delete associated dataHubMetricCube
    try {
      String serializedMonitorUrn =
          Base64.getEncoder().encodeToString(monitorUrn.toString().getBytes());
      Urn metricCubeUrn =
          Urn.createFromString(String.format("urn:li:dataHubMetricCube:%s", serializedMonitorUrn));
      log.info(
          String.format(
              "Found a dataHubMetricCube associated with monitor being removed urn %s. Removing dataHubMetricCube %s",
              monitorUrn, metricCubeUrn));
      entityClient.deleteEntity(systemOperationContext, metricCubeUrn);
    } catch (Exception e) {
      log.error(
          String.format(
              "Caught exception while attempting to delete dataHubMetricCube associated with removed monitor urn %s! "
                  + "This means that a stale dataHubMetricCube may remain.",
              monitorUrn),
          e);
    }
  }

  private void handleAssertionHardDeleteEvent(Urn assertionUrn) {
    // find linked monitor, if any
    Urn monitorUrn = MonitorUtils.getMonitorUrnForAssertion(graphClient, assertionUrn);
    // delete monitor
    if (monitorUrn != null) {
      log.info(
          String.format(
              "Found a monitor associated with assertion being removed urn %s. Removing monitor %s",
              assertionUrn, monitorUrn));
      try {
        entityClient.deleteEntity(systemOperationContext, monitorUrn);
      } catch (RemoteInvocationException e) {
        log.error(
            String.format(
                "Caught exception while attempting to delete monitor %s associated with removed assertion urn %s! "
                    + "This means that a stale monitor may remain active.",
                monitorUrn, assertionUrn),
            e);
      }
    }
  }

  private void handleDatasetHardDeleteEvent(Urn assertionUrn) {
    // Find linked monitors, if any.
    List<Urn> monitors = MonitorUtils.getMonitorUrnsForDataset(graphClient, assertionUrn);
    // delete monitors
    if (monitors != null) {
      log.info(
          String.format(
              "Found monitor URNs associated with dataset being removed urn %s. Removing monitors %s",
              assertionUrn, monitors));
      try {
        for (final Urn urn : monitors) {
          entityClient.deleteEntity(systemOperationContext, urn);
        }
      } catch (RemoteInvocationException e) {
        log.error(
            String.format(
                "Caught exception while attempting to delete monitors %s associated with removed dataset urn %s! "
                    + "This means that one or more stale monitors may remain active.",
                monitors, assertionUrn),
            e);
      }
    }
  }

  private boolean isAssertionHardDeletionEvent(@Nonnull final MetadataChangeLog event) {
    return ChangeType.DELETE.equals(event.getChangeType())
        && ASSERTION_KEY_ASPECT_NAME.equals(event.getAspectName());
  }

  private boolean isMonitorHardDeletionEvent(@Nonnull final MetadataChangeLog event) {
    return ChangeType.DELETE.equals(event.getChangeType())
        && MONITOR_KEY_ASPECT_NAME.equals(event.getAspectName());
  }

  private boolean isDatasetHardDeletionEvent(@Nonnull final MetadataChangeLog event) {
    return ChangeType.DELETE.equals(event.getChangeType())
        && DATASET_KEY_ASPECT_NAME.equals(event.getAspectName());
  }
}
