package com.linkedin.metadata.monitor;

import static com.linkedin.metadata.Constants.MONITOR_INFO_ASPECT_NAME;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Mutation hook that overrides the evaluation schedule on MonitorInfo aspects when a time bucketing
 * strategy is configured.
 *
 * <p>This ensures monitors created via REST API or direct MCP ingestion (bypassing GraphQL) still
 * get the correct bucket-aligned schedule. The GraphQL resolvers also derive the schedule, but this
 * hook is the authoritative enforcement for all write paths.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class MonitorScheduleMutationHook extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream().map(this::maybeOverrideSchedule);
  }

  private Pair<ChangeMCP, Boolean> maybeOverrideSchedule(ChangeMCP changeMCP) {
    if (!MONITOR_INFO_ASPECT_NAME.equals(changeMCP.getAspectName())) {
      return Pair.of(changeMCP, false);
    }

    final MonitorInfo monitorInfo = changeMCP.getAspect(MonitorInfo.class);
    if (monitorInfo == null) {
      return Pair.of(changeMCP, false);
    }

    final boolean mutated = MonitorBucketingUtils.maybeOverrideSchedule(monitorInfo);
    if (mutated) {
      log.info(
          "Overrode evaluation schedule with bucket-aligned schedule for monitor {}",
          changeMCP.getUrn());
    }
    return Pair.of(changeMCP, mutated);
  }
}
