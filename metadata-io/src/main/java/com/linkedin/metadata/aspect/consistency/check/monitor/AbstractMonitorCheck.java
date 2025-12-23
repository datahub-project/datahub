package com.linkedin.metadata.aspect.consistency.check.monitor;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.AbstractEntityCheck;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class for monitor consistency checks.
 *
 * <p>Provides common functionality for monitor checks:
 *
 * <ul>
 *   <li>Default required aspects for monitor entity type
 *   <li>Extraction of MonitorInfo and AssertionMonitor from entity response
 *   <li>Validation of entity types valid for monitors (derived from PDL annotations)
 *   <li>Helper methods for common monitor validation patterns
 * </ul>
 *
 * <p>Subclasses implement {@link #checkMonitor} to perform the specific check logic. Use {@link
 * com.linkedin.metadata.utils.MonitorUrnUtils#getEntityUrn} to extract entity URN from monitor URN.
 */
@Slf4j
public abstract class AbstractMonitorCheck extends AbstractEntityCheck {

  /** Default aspects required by monitor checks */
  protected static final Set<String> DEFAULT_MONITOR_ASPECTS =
      Set.of(MONITOR_INFO_ASPECT_NAME, STATUS_ASPECT_NAME);

  /** Valid entity types for monitor entities (computed once from entity registry) */
  @Getter private final Set<String> validMonitorEntityTypes;

  /**
   * Construct with entity registry to derive valid monitor entity types.
   *
   * @param entityRegistry the entity registry for deriving valid entity types from PDL annotations
   */
  protected AbstractMonitorCheck(@Nonnull EntityRegistry entityRegistry) {
    this.validMonitorEntityTypes =
        Collections.unmodifiableSet(
            computeValidEntityTypesFromAspect(
                entityRegistry, MONITOR_ENTITY_NAME, MONITOR_KEY_ASPECT_NAME));
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return MONITOR_ENTITY_NAME;
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    return Optional.of(DEFAULT_MONITOR_ASPECTS);
  }

  @Override
  @Nonnull
  protected final List<ConsistencyIssue> checkEntity(
      @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {
    return checkMonitor(ctx, urn, response);
  }

  /**
   * Perform the specific consistency check on a monitor.
   *
   * @param ctx check context
   * @param monitorUrn URN of the monitor
   * @param response entity response
   * @return list of issues found
   */
  @Nonnull
  protected abstract List<ConsistencyIssue> checkMonitor(
      @Nonnull CheckContext ctx, @Nonnull Urn monitorUrn, @Nonnull EntityResponse response);

  /**
   * Extract MonitorInfo from the response.
   *
   * @param response entity response
   * @return MonitorInfo or null if not present
   */
  @Nullable
  protected MonitorInfo getMonitorInfo(@Nonnull EntityResponse response) {
    EnvelopedAspect infoAspect = getAspect(response, MONITOR_INFO_ASPECT_NAME);
    if (infoAspect == null) {
      return null;
    }
    return new MonitorInfo(infoAspect.getValue().data());
  }

  /**
   * Extract AssertionMonitor from MonitorInfo.
   *
   * @param monitorInfo the monitor info
   * @return AssertionMonitor or null if not present
   */
  @Nullable
  protected AssertionMonitor getAssertionMonitor(@Nullable MonitorInfo monitorInfo) {
    if (monitorInfo == null || !monitorInfo.hasAssertionMonitor()) {
      return null;
    }
    return monitorInfo.getAssertionMonitor();
  }

  /**
   * Check if the entity type is valid for the entity referenced by a monitor.
   *
   * <p>Valid entity types are derived from @UrnValidation annotations in the monitorKey aspect PDL
   * model at construction time.
   *
   * @param entityType entity type string to validate
   * @return true if valid
   */
  protected boolean isValidMonitorEntityType(String entityType) {
    return validMonitorEntityTypes.contains(entityType);
  }
}
