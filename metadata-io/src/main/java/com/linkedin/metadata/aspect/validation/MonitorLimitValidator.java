package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.MONITOR_ENTITY_NAME;
import static com.linkedin.metadata.Constants.MONITOR_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.search.ScrollResult;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validator that enforces a global limit on the number of monitor entities that can be created.
 * This prevents unlimited creation of monitor entities which could impact system performance.
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class MonitorLimitValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  private boolean enabled = true;
  private int maxMonitors = 1000; // Default limit

  public MonitorLimitValidator setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  public MonitorLimitValidator setMaxMonitors(int maxMonitors) {
    this.maxMonitors = maxMonitors;
    return this;
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    if (!enabled) {
      return Stream.empty();
    }

    // Only validate monitor entity creation/upsert operations
    final List<? extends BatchItem> monitorItems =
        mcpItems.stream()
            .filter(item -> MONITOR_ENTITY_NAME.equals(item.getUrn().getEntityType()))
            .filter(item -> MONITOR_INFO_ASPECT_NAME.equals(item.getAspectName()))
            .filter(
                item ->
                    item.getChangeType().name().equals("CREATE")
                        || item.getChangeType().name().equals("CREATE_ENTITY")
                        || item.getChangeType().name().equals("UPSERT"))
            .collect(Collectors.toList());

    if (monitorItems.isEmpty()) {
      return Stream.empty();
    }

    try {
      // Count existing monitor entities
      final int existingMonitorCount = countExistingMonitors(retrieverContext);

      // Get items that would create new monitors
      final List<? extends BatchItem> newMonitorItems =
          getNewMonitorItems(monitorItems, retrieverContext);
      final int newMonitorsCount = newMonitorItems.size();

      if (existingMonitorCount + newMonitorsCount > maxMonitors) {
        log.warn(
            "Monitor limit validation failed. Current monitors: {}, attempting to create: {}, limit: {}",
            existingMonitorCount,
            newMonitorsCount,
            maxMonitors);

        // Return exceptions for items that would actually create new monitors
        return newMonitorItems.stream()
            .map(
                item ->
                    AspectValidationException.forItem(
                        item,
                        String.format(
                            "%s Current monitors: %d, attempting to create: %d, maximum allowed: %d. "
                                + "Cannot create new monitor entity.",
                            AcrylConstants.MONITOR_LIMIT_EXCEEDED_ERROR_MESSAGE_PREFIX,
                            existingMonitorCount,
                            newMonitorsCount,
                            maxMonitors)));
      }
    } catch (Exception e) {
      log.error("Error validating monitor limit", e);
      // In case of error, allow the operation but log the issue
      return Stream.empty();
    }

    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    // No pre-commit validation needed for monitor limits
    return Stream.empty();
  }

  /**
   * Counts the existing monitor entities in the system.
   *
   * @param retrieverContext the context for accessing data
   * @return the number of existing monitor entities
   */
  private int countExistingMonitors(RetrieverContext retrieverContext) {
    try {
      // Use the search retriever to count monitor entities
      final ScrollResult result =
          retrieverContext
              .getSearchRetriever()
              .scroll(
                  List.of(MONITOR_ENTITY_NAME),
                  null, // No filter to get all monitors
                  null, // scrollId
                  1 // We only need the count, not the actual entities
                  );

      if (result == null) {
        log.warn("Search result is null, defaulting to 0 monitor count");
        return 0;
      }

      return result.getNumEntities();
    } catch (Exception e) {
      log.error("Failed to count existing monitors, defaulting to 0", e);
      return 0; // Default to 0 on error to be safe
    }
  }

  /**
   * Gets the list of monitor items that would actually create new monitors. This excludes UPSERT
   * operations on existing entities and ensures unique entities only.
   *
   * @param monitorItems the batch items for monitor operations
   * @param retrieverContext the context for accessing data
   * @return the list of items that would create new monitors
   */
  private List<? extends BatchItem> getNewMonitorItems(
      List<? extends BatchItem> monitorItems, RetrieverContext retrieverContext) {
    try {
      // Get unique monitor URNs from the batch
      final Set<Urn> uniqueMonitorUrns =
          monitorItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());

      // Check which monitors already exist
      final Map<Urn, Boolean> existenceMap =
          retrieverContext.getAspectRetriever().entityExists(uniqueMonitorUrns);

      // Track which URNs we've already processed to avoid duplicates
      final Set<Urn> processedUrns = new java.util.HashSet<>();

      return monitorItems.stream()
          .filter(
              item -> {
                final Urn urn = item.getUrn();
                final boolean exists = existenceMap.getOrDefault(urn, false);

                // Only include items that don't exist yet and haven't been processed
                if (!exists && !processedUrns.contains(urn)) {
                  processedUrns.add(urn);
                  return true;
                }
                return false;
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Failed to get new monitor items, defaulting to all items for safety", e);
      // On error, be conservative and assume all items would create new monitors
      return monitorItems;
    }
  }
}
