package com.linkedin.metadata.kafka.hook.monitor;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.r2.RemoteInvocationException;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
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
@Singleton
@Import({EntityRegistryFactory.class})
public class MonitorDeletionHook implements MetadataChangeLogHook {

  private final EntityRegistry _entityRegistry;
  private final GraphClient _graphClient;
  private final EntityClient _entityClient;
  private final Authentication _authentication;
  private final Boolean _isEnabled;

  @Autowired
  public MonitorDeletionHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull @Value("${monitorDeletion.hook.enabled:true}") Boolean isEnabled) {
    _entityRegistry = Objects.requireNonNull(entityRegistry, "entityRegistry is required");
    _entityClient = Objects.requireNonNull(systemEntityClient, "entityClient is required");
    _graphClient = Objects.requireNonNull(graphClient, "graphClient is required");
    _authentication =
        Objects.requireNonNull(
            systemEntityClient.getSystemAuthentication(), "authentication is required");
    _isEnabled = Objects.requireNonNull(isEnabled, "isEnabled is required");
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
    if (_isEnabled) {
      log.debug("Urn {} received by Delete Monitor Hook.", mcl.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(mcl, _entityRegistry);
      if (isAssertionHardDeletionEvent(mcl)) {
        handleAssertionHardDeleteEvent(urn);
      } else if (isMonitorHardDeletionEvent(mcl)) {
        handleMonitorHardDeleteEvent(urn);
      }
    }
  }

  private void handleMonitorHardDeleteEvent(Urn monitorUrn) {
    // find linked assertion, if any
    Urn assertionUrn = MonitorUtils.getAssertionUrnForMonitor(_graphClient, monitorUrn);
    // delete assertion
    if (assertionUrn != null) {
      log.info(
          String.format(
              "Found an assertion associated with monitor being removed urn %s. Removing assertion %s",
              monitorUrn, assertionUrn));
      try {
        _entityClient.deleteEntity(assertionUrn, _authentication);
      } catch (RemoteInvocationException e) {
        log.error(
            String.format(
                "Caught exception while attempting to delete assertion %s associated with removed monitor urn %s! "
                    + "This means that a stale assertion may remain.",
                assertionUrn, monitorUrn),
            e);
      }
    }
  }

  private void handleAssertionHardDeleteEvent(Urn assertionUrn) {
    // find linked monitor, if any
    Urn monitorUrn = MonitorUtils.getMonitorUrnForAssertion(_graphClient, assertionUrn);
    // delete monitor
    if (monitorUrn != null) {
      log.info(
          String.format(
              "Found a monitor associated with assertion being removed urn %s. Removing monitor %s",
              assertionUrn, monitorUrn));
      try {
        _entityClient.deleteEntity(monitorUrn, _authentication);
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

  private boolean isAssertionHardDeletionEvent(@Nonnull final MetadataChangeLog event) {
    return ChangeType.DELETE.equals(event.getChangeType())
        && ASSERTION_KEY_ASPECT_NAME.equals(event.getAspectName());
  }

  private boolean isMonitorHardDeletionEvent(@Nonnull final MetadataChangeLog event) {
    return ChangeType.DELETE.equals(event.getChangeType())
        && MONITOR_KEY_ASPECT_NAME.equals(event.getAspectName());
  }
}
