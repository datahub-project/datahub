package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Pages the source through this process and bulk-writes to the target, each leg through the
 * cluster's own client, so every connection configuration works and nothing is asked of either
 * cluster. A partition is one scroll over the source sorted by {@code _doc}: exact on every engine
 * version in use (OpenSearch 2.x has no {@code _shard_doc} tiebreaker for point-in-time paging),
 * and every page is acknowledged item by item before the next is read, so the counts are exact.
 *
 * <p>The scroll id is the resume cursor, and it is only safe to resume from right after a page was
 * written: the position lives on the cluster and moves with every fetch, so a cursor checkpointed
 * before a crash may already be past a page the crash lost. A paused copy therefore checkpoints and
 * stops before fetching further, while a failed one starts the partition over. A context the
 * cluster has dropped also restarts the partition, which the write semantics make safe.
 */
@Slf4j
public final class InProcessCopier implements CrossClusterCopier {
  public static final String NAME = "inProcess";
  private static final String NDJSON = "application/x-ndjson";

  /**
   * Replaces the target's document with the source's unless the target's is current: newer by the
   * guard field; stamped where the source's copy is not, since the write that stamped it came after
   * the source was read (a graph edge gains {@code updatedOn} on its first update); or the same by
   * content, including when the guard is missing on both sides. A graph edge selected by {@code
   * createdOn} alone has no {@code updatedOn}, and rewriting it on every pass would never come out
   * clean. On a document the target lacks the upsert runs this over an empty source, which the else
   * branch fills.
   */
  static final String GUARDED_REPLACE_SCRIPT =
      "def mine = ctx._source[params.field];"
          + " boolean sameStamp = (mine == null && params.value == null)"
          + " || (mine != null && params.value != null && mine == params.value);"
          + " if ((sameStamp && ctx._source == params.doc)"
          + " || (mine != null && (params.value == null || mine > params.value))) { ctx.op = 'none' }"
          + " else { ctx._source.clear(); ctx._source.putAll(params.doc) }";

  // A mirrored write can land between the script's read and its write; the retry re-evaluates it.
  private static final int RETRIES_ON_CONFLICT = 3;

  private final ClusterHttp source;
  private final ClusterHttp target;
  private final int pageSize;
  private final String keepAlive;
  private final int checkpointEveryPages;
  private final Pacer pacer;

  /** Slows the copy down when the target is under pressure; the default never waits. */
  @FunctionalInterface
  public interface Pacer {
    Pacer NONE = () -> {};

    void awaitCapacity() throws InterruptedException;
  }

  public InProcessCopier(
      @Nonnull final ClusterHttp source,
      @Nonnull final ClusterHttp target,
      final int pageSize,
      @Nonnull final String keepAlive,
      final int checkpointEveryPages,
      @Nonnull final Pacer pacer) {
    this.source = source;
    this.target = target;
    this.pageSize = Math.max(1, pageSize);
    this.keepAlive = keepAlive;
    this.checkpointEveryPages = Math.max(1, checkpointEveryPages);
    this.pacer = pacer;
  }

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public Result copy(
      @Nonnull final CopyPartition partition,
      @Nullable final Checkpoint resumeFrom,
      @Nonnull final Checkpointer checkpointer,
      @Nonnull final BooleanSupplier shouldStop)
      throws IOException, InterruptedException {
    long read = 0;
    long created = 0;
    long alreadyPresent = 0;
    long failures = 0;
    JsonNode page = null;
    if (resumeFrom != null && resumeFrom.cursor() != null) {
      page = continueScroll(resumeFrom.cursor());
      if (page == null) {
        log.info(
            "Scroll {} for {} is gone; starting the partition over",
            resumeFrom.cursor(),
            partition.label());
      } else {
        read = resumeFrom.read();
        created = resumeFrom.created();
        alreadyPresent = resumeFrom.alreadyPresent();
        failures = resumeFrom.failures();
      }
    }
    if (page == null) {
      page = openScroll(partition);
    }
    final long total = page.path("hits").path("total").path("value").asLong(-1);
    int pagesSinceCheckpoint = 0;
    String scrollId = page.path("_scroll_id").asText();
    boolean paused = false;
    try {
      while (true) {
        final JsonNode hits = page.path("hits").path("hits");
        if (hits.isEmpty()) {
          break;
        }
        pacer.awaitCapacity();
        final BulkOutcome outcome = bulkWrite(partition, hits);
        read += hits.size();
        created += outcome.created;
        alreadyPresent += outcome.alreadyPresent;
        failures += outcome.failures;
        pagesSinceCheckpoint++;
        if (hits.size() < pageSize) {
          break;
        }
        final boolean stop = shouldStop.getAsBoolean();
        if (pagesSinceCheckpoint >= checkpointEveryPages || stop) {
          checkpointer.checkpoint(
              new Checkpoint(scrollId, read, created, alreadyPresent, failures));
          pagesSinceCheckpoint = 0;
        }
        if (stop) {
          log.info(
              "Pausing copy of {} after {} documents ({} created, {} present, {} failed); scroll"
                  + " {} stays open for {}",
              partition.label(),
              read,
              created,
              alreadyPresent,
              failures,
              scrollId,
              keepAlive);
          paused = true;
          return new Result(total, read, created, alreadyPresent, failures, true);
        }
        page = continueScroll(scrollId);
        if (page == null) {
          throw new IOException(
              "Scroll " + scrollId + " for " + partition.label() + " expired mid-copy");
        }
        scrollId = page.path("_scroll_id").asText(scrollId);
      }
    } finally {
      // A paused partition keeps its scroll so the next run can continue it.
      if (!paused) {
        closeScroll(scrollId);
      }
    }
    final Result result = new Result(total, read, created, alreadyPresent, failures, false);
    log.info(
        "Copied {} -> {} ({}): read {}, created {}, present {}, failed {}, source total {}",
        partition.sourceIndex(),
        partition.targetIndex(),
        partition.label(),
        read,
        created,
        alreadyPresent,
        failures,
        total);
    return result;
  }

  @Nonnull
  private JsonNode openScroll(@Nonnull final CopyPartition partition) throws IOException {
    final ObjectNode body = source.object();
    body.put("size", pageSize);
    if (partition.filter() != null) {
      body.set("query", partition.filter());
    }
    body.putArray("sort").add("_doc");
    body.put("track_total_hits", true);
    return source.json(
        "POST", "/" + partition.sourceIndex() + "/_search?scroll=" + keepAlive, body);
  }

  /**
   * The next page, or null when the cluster no longer knows the scroll or cannot read the id at
   * all. A cursor that is not one of this cluster's scroll ids (another copier's, or another
   * cluster's) is as gone as an expired one, and the partition starts over.
   */
  @Nullable
  private JsonNode continueScroll(@Nonnull final String scrollId) throws IOException {
    final ObjectNode body = source.object();
    body.put("scroll", keepAlive);
    body.put("scroll_id", scrollId);
    final ClusterHttp.Answer answer = source.answer("POST", "/_search/scroll", body, "400", "404");
    if (answer.status() == 404) {
      return null;
    }
    if (answer.status() == 400) {
      if (answer.body().path("error").path("reason").asText().contains("Cannot parse scroll id")) {
        log.warn("Scroll id {} is not one the source can parse; treating it as gone", scrollId);
        return null;
      }
      throw new IOException("Scroll " + scrollId + " could not be continued: " + answer.body());
    }
    return answer.body();
  }

  private void closeScroll(@Nullable final String scrollId) {
    if (scrollId == null || scrollId.isEmpty()) {
      return;
    }
    try {
      final ObjectNode body = source.object();
      body.putArray("scroll_id").add(scrollId);
      source.send("DELETE", "/_search/scroll", body, "404");
    } catch (Exception e) {
      log.warn("Could not clear scroll {}: {}", scrollId, e.toString());
    }
  }

  private record BulkOutcome(long created, long alreadyPresent, long failures) {}

  @Nonnull
  private BulkOutcome bulkWrite(
      @Nonnull final CopyPartition partition, @Nonnull final JsonNode hits) throws IOException {
    final StringBuilder ndjson = new StringBuilder(hits.size() * 512);
    for (JsonNode hit : hits) {
      final String id = hit.path("_id").asText();
      final JsonNode document = hit.path("_source");
      if (partition.guardField() == null) {
        final ObjectNode action = target.object();
        action.putObject("create").put("_index", partition.targetIndex()).put("_id", id);
        ndjson.append(target.getMapper().writeValueAsString(action)).append('\n');
        ndjson.append(target.getMapper().writeValueAsString(document)).append('\n');
      } else {
        final ObjectNode action = target.object();
        action
            .putObject("update")
            .put("_index", partition.targetIndex())
            .put("_id", id)
            .put("retry_on_conflict", RETRIES_ON_CONFLICT);
        final ObjectNode body = target.object();
        body.put("scripted_upsert", true);
        final ObjectNode script = body.putObject("script");
        script.put("source", GUARDED_REPLACE_SCRIPT).put("lang", "painless");
        final ObjectNode params = script.putObject("params");
        params.put("field", partition.guardField());
        params.set("value", document.get(partition.guardField()));
        params.set("doc", document);
        body.putObject("upsert");
        ndjson.append(target.getMapper().writeValueAsString(action)).append('\n');
        ndjson.append(target.getMapper().writeValueAsString(body)).append('\n');
      }
    }
    final JsonNode response = target.raw("POST", "/_bulk", ndjson.toString(), NDJSON);
    long created = 0;
    long alreadyPresent = 0;
    long failures = 0;
    String firstError = null;
    for (JsonNode item : response.path("items")) {
      final JsonNode result = actionResult(item);
      final int status = result.path("status").asInt();
      if (status == 201 || (status == 200 && "updated".equals(result.path("result").asText()))) {
        // Created, or a guarded replace that wrote the source's version over an older one: either
        // way the target lacked what the source holds.
        created++;
      } else if (status == 200 || status == 409) {
        // A create the target already had; a guarded replace that found the target current
        // (noop); or one that lost every conflict retry to live mirrored writes of the same
        // document, which are newer than anything read here.
        alreadyPresent++;
      } else {
        failures++;
        if (firstError == null) {
          firstError = result.path("error").toString();
        }
      }
    }
    if (failures > 0) {
      log.error(
          "Bulk write into {} rejected {} of {} documents; first error: {}",
          partition.targetIndex(),
          failures,
          hits.size(),
          firstError);
    }
    return new BulkOutcome(created, alreadyPresent, failures);
  }

  /** The one action's result inside a bulk item ({@code create} or {@code update}). */
  @Nonnull
  private static JsonNode actionResult(@Nonnull final JsonNode item) {
    final Iterator<JsonNode> results = item.elements();
    return results.hasNext() ? results.next() : item;
  }
}
