package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Compares the source and the target once a family's copy is complete: a count per logical index,
 * advisory because the live source keeps changing, and an exact check that the source's most
 * recently written documents exist on the target. Those were mirrored, so after a refresh they
 * should all be there; one that is not, after a few re-checks that cover a mirrored write still in
 * flight, was lost by the mirror. Both are recorded on the migration row and reported, and the
 * number still missing is returned for the caller to act on.
 */
@Slf4j
final class CopyVerifier {
  static final int SAMPLE = 25;
  static final int RECHECKS = 3;

  private final OperationContext opContext;
  private final ClusterHttp source;
  private final ClusterHttp target;
  private final MigrationFamily family;
  private final String stepId;
  private final long recheckPauseMs;

  /**
   * @param recheckPauseMs how long to wait before looking for a missing document again, so a
   *     mirrored write that the target's bulk processor has not flushed yet is not counted as lost
   */
  CopyVerifier(
      @Nonnull final OperationContext opContext,
      @Nonnull final ClusterHttp source,
      @Nonnull final ClusterHttp target,
      @Nonnull final MigrationFamily family,
      @Nonnull final String stepId,
      final long recheckPauseMs) {
    this.opContext = opContext;
    this.source = source;
    this.target = target;
    this.family = family;
    this.stepId = stepId;
    this.recheckPauseMs = recheckPauseMs;
  }

  /**
   * @return how many of the sampled source documents the target still lacks, over every index
   */
  int verify(
      @Nonnull final UpgradeContext context,
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final List<SourceIndexSet> sourceSets)
      throws IOException, InterruptedException {
    int missingInAll = 0;
    for (SourceIndexSet set : sourceSets) {
      final String targetLogical =
          set.isRollover()
              ? migration.getNames().targetToken(opContext) + Constants.DATAHUB_USAGE_EVENT_INDEX
              : migration.getNames().target(opContext, set.logicalName());
      // Once per index on each side, so the counts and the sample see every write so far.
      source.refresh(set.logicalName());
      target.refresh(targetLogical);
      final long sourceCount = source.count(set.logicalName(), null);
      final long targetCount = target.count(targetLogical, null);
      final int missing = sampleMissing(set.logicalName(), targetLogical);
      missingInAll += missing;
      final String prefix = ClusterMigrationState.VERIFY_PREFIX + set.logicalName() + ".";
      row.put(prefix + "sourceCount", sourceCount);
      row.put(prefix + "targetCount", targetCount);
      row.put(prefix + "sampledMissing", missing);
      row.put(prefix + "at", System.currentTimeMillis());
      final String line =
          String.format(
              "%s: %s source=%d target=%d (live source keeps changing; advisory) sampledMissing=%d/%d",
              stepId, set.logicalName(), sourceCount, targetCount, missing, SAMPLE);
      log.info(line);
      context.report().addLine(line);
    }
    return missingInAll;
  }

  /**
   * How many of the source's most recently written documents the target lacks, looked for again
   * after a pause while any are missing, up to {@link #RECHECKS} times.
   */
  private int sampleMissing(@Nonnull final String sourceIndex, @Nonnull final String targetIndex)
      throws IOException, InterruptedException {
    final ObjectNode body = source.object();
    body.put("size", SAMPLE);
    body.put("_source", false);
    body.putArray("sort").addObject().put(family.timeField(), "desc");
    final JsonNode hits =
        source.json("POST", "/" + sourceIndex + "/_search", body).path("hits").path("hits");
    final List<String> ids = new ArrayList<>();
    for (JsonNode hit : hits) {
      ids.add(hit.path("_id").asText());
    }
    if (ids.isEmpty()) {
      return 0;
    }
    Set<String> missing = missingOnTarget(targetIndex, ids);
    for (int recheck = 1; !missing.isEmpty() && recheck < RECHECKS; recheck++) {
      Thread.sleep(recheckPauseMs);
      target.refresh(targetIndex);
      missing = missingOnTarget(targetIndex, new ArrayList<>(missing));
    }
    if (!missing.isEmpty()) {
      // A sampled document the source no longer holds was deleted since the sample was taken (graph
      // edges are replaced on every ingestion run, and the newest are the ones in flux); the
      // mirror removed it from the target, which is not a loss.
      source.refresh(sourceIndex);
      missing.retainAll(presentOn(source, sourceIndex, new ArrayList<>(missing)));
    }
    if (!missing.isEmpty()) {
      log.warn(
          "{}: {} of the {} newest documents of {} are missing on {} after {} looks: {}",
          stepId,
          missing.size(),
          ids.size(),
          sourceIndex,
          targetIndex,
          RECHECKS,
          missing);
    }
    return missing.size();
  }

  @Nonnull
  private Set<String> missingOnTarget(
      @Nonnull final String targetIndex, @Nonnull final List<String> ids) throws IOException {
    final Set<String> missing = new HashSet<>(ids);
    missing.removeAll(presentOn(target, targetIndex, ids));
    return missing;
  }

  /** The ids among {@code ids} that the index holds; ids travel in the body, never in a path. */
  @Nonnull
  private static Set<String> presentOn(
      @Nonnull final ClusterHttp cluster,
      @Nonnull final String index,
      @Nonnull final List<String> ids)
      throws IOException {
    final ObjectNode lookup = cluster.object();
    lookup.put("size", ids.size());
    lookup.put("_source", false);
    final ArrayNode values = lookup.putObject("query").putObject("ids").putArray("values");
    ids.forEach(values::add);
    final Set<String> present = new HashSet<>();
    for (JsonNode hit :
        cluster.json("POST", "/" + index + "/_search", lookup).path("hits").path("hits")) {
      present.add(hit.path("_id").asText());
    }
    return present;
  }
}
