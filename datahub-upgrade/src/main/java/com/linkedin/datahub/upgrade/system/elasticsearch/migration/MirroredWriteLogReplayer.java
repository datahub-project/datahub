package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.search.elasticsearch.update.MirroredWriteLog;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Brings the target back in line with the source for every document a mirrored delete or by-query
 * operation could have touched while the copy ran. A copy partition can read a document, see the
 * mirror delete it on both clusters, and then create it on the target. A document the live writers
 * delete and write again keeps whatever timestamp its aspect carries, so no clock can tell such a
 * document from a resurrected one; the source can, since it is the family's truth until the flip.
 * For each logged entry the documents it matches, on the target and on the source, are compared:
 * those the source lacks are deleted from the target, those it holds are written there as the
 * source has them. The script of a logged update is therefore not re-applied; the source's version
 * of the document replaces whatever the update would have produced. Matching on the source as well
 * is what restores a document a by-query delete removed from the target but not from the source (an
 * asynchronous one that failed there after it was mirrored); the target's side of the match can no
 * longer name it. Entries are idempotent, so a replay interrupted halfway is simply run again.
 */
@Slf4j
public final class MirroredWriteLogReplayer {
  private static final int PAGE = 500;
  private static final int MATCH_PAGE = 1000;
  private static final int RECONCILE_BATCH = 500;
  private static final String SCROLL_KEEP_ALIVE = "2m";

  private final ClusterHttp target;
  private final ClusterHttp source;
  private final Map<String, String> sourceIndexByTarget;
  @Nullable private final String guardField;

  /** Ids of target documents a logged entry could have touched, not yet reconciled, per index. */
  private final Map<String, Set<String>> pending = new LinkedHashMap<>();

  private int pendingCount;
  private long matched;
  private long deleted;
  private long rewritten;
  private long leftAlone;

  /**
   * @param sourceIndexByTarget the source index holding each target index's documents, for every
   *     index the mirror can have logged
   */
  public MirroredWriteLogReplayer(
      @Nonnull final ClusterHttp target,
      @Nonnull final ClusterHttp source,
      @Nonnull final Map<String, String> sourceIndexByTarget) {
    this(target, source, sourceIndexByTarget, null);
  }

  /**
   * @param guardField the family's version field, so a target document stamped later than the
   *     source's copy is left as it is. Null compares nothing and writes the source's version.
   */
  public MirroredWriteLogReplayer(
      @Nonnull final ClusterHttp target,
      @Nonnull final ClusterHttp source,
      @Nonnull final Map<String, String> sourceIndexByTarget,
      @Nullable final String guardField) {
    this.target = target;
    this.source = source;
    this.sourceIndexByTarget = sourceIndexByTarget;
    this.guardField = guardField;
  }

  /**
   * @param fromTs replay entries with {@code ts >= fromTs}
   * @return entries replayed
   */
  public long replay(@Nonnull final String logIndex, final long fromTs) throws IOException {
    if (!target.indexExists(logIndex)) {
      log.info("No mirrored write log {}; nothing to replay", logIndex);
      return 0;
    }
    target.refresh(logIndex);
    long replayed = 0;
    JsonNode searchAfter = null;
    while (true) {
      final ObjectNode body = target.object();
      body.put("size", PAGE);
      body.putObject("query")
          .putObject("range")
          .putObject(MirroredWriteLog.FIELD_TS)
          .put("gte", fromTs);
      final ArrayNode sort = body.putArray("sort");
      sort.addObject().put(MirroredWriteLog.FIELD_TS, "asc");
      sort.add("_doc");
      if (searchAfter != null) {
        body.set("search_after", searchAfter);
      }
      final JsonNode hits =
          target.json("POST", "/" + logIndex + "/_search", body).path("hits").path("hits");
      if (hits.isEmpty()) {
        break;
      }
      for (JsonNode hit : hits) {
        collect(hit.path("_source"));
        replayed++;
      }
      searchAfter = hits.get(hits.size() - 1).path("sort");
      if (hits.size() < PAGE) {
        break;
      }
    }
    reconcileAll();
    log.info(
        "Replayed {} mirrored write log entries from {} (ts >= {}): {} documents deleted, {}"
            + " written again from the source, {} left to a mirrored write that got there first",
        replayed,
        logIndex,
        fromTs,
        deleted,
        rewritten,
        leftAlone);
    return replayed;
  }

  /**
   * Reconciles every document on the target that {@code query} matches, the way a logged by-query
   * entry is. The copy step hands it the documents the target received while the previous rollout's
   * pods were still writing: a delete one of them made without the mirror leaves a document only
   * the target holds, nothing logged it, and the source alone can tell.
   *
   * @return documents the query matched on the target
   */
  public long reconcileMatching(@Nonnull final String targetIndex, @Nonnull final ObjectNode query)
      throws IOException {
    final long before = matched;
    rememberMatching(target, targetIndex, targetIndex, query.toString());
    reconcileAll();
    log.info(
        "Reconciled {} documents of {} matching {}: {} deleted, {} written again from the source,"
            + " {} left to a mirrored write that got there first",
        matched - before,
        targetIndex,
        query,
        deleted,
        rewritten,
        leftAlone);
    return matched - before;
  }

  private void collect(@Nonnull final JsonNode entry) throws IOException {
    final String kind = entry.path(MirroredWriteLog.FIELD_KIND).asText();
    final String index = entry.path(MirroredWriteLog.FIELD_INDEX).asText();
    switch (MirroredWriteLog.Kind.valueOf(kind)) {
      case DELETE_BY_ID:
        remember(index, entry.path(MirroredWriteLog.FIELD_ID).asText());
        break;
      case DELETE_BY_QUERY:
      case UPDATE_BY_QUERY:
        final String query = entry.path(MirroredWriteLog.FIELD_QUERY).asText();
        rememberMatching(target, index, index, query);
        rememberMatching(source, sourceIndexOf(index), index, query);
        break;
      default:
        throw new IOException("Unknown mirrored write log entry kind " + kind);
    }
  }

  private void remember(@Nonnull final String index, @Nonnull final String id) throws IOException {
    pending.computeIfAbsent(index, i -> new LinkedHashSet<>()).add(id);
    if (++pendingCount >= RECONCILE_BATCH) {
      reconcileAll();
    }
  }

  /**
   * Remembers every id the query matches on {@code cluster} now, page by page, so one wide
   * operation never holds all of its ids at once; ids only travel in request bodies, never in
   * paths.
   */
  private void rememberMatching(
      @Nonnull final ClusterHttp cluster,
      @Nonnull final String index,
      @Nonnull final String targetIndex,
      @Nonnull final String queryJson)
      throws IOException {
    final ObjectNode body = cluster.object();
    body.put("size", MATCH_PAGE);
    body.put("_source", false);
    body.set("query", cluster.getMapper().readTree(queryJson));
    body.putArray("sort").add("_doc");
    JsonNode page =
        cluster.jsonOrNull("POST", "/" + index + "/_search?scroll=" + SCROLL_KEEP_ALIVE, body);
    if (page == null) {
      return;
    }
    String scrollId = page.path("_scroll_id").asText();
    try {
      while (true) {
        final JsonNode hits = page.path("hits").path("hits");
        for (JsonNode hit : hits) {
          matched++;
          remember(targetIndex, hit.path("_id").asText());
        }
        if (hits.size() < MATCH_PAGE) {
          break;
        }
        final ObjectNode next = cluster.object();
        next.put("scroll", SCROLL_KEEP_ALIVE);
        next.put("scroll_id", scrollId);
        page = cluster.json("POST", "/_search/scroll", next);
        scrollId = page.path("_scroll_id").asText(scrollId);
      }
    } finally {
      if (!scrollId.isEmpty()) {
        final ObjectNode clear = cluster.object();
        clear.putArray("scroll_id").add(scrollId);
        cluster.send("DELETE", "/_search/scroll", clear, "404");
      }
    }
  }

  @Nonnull
  private String sourceIndexOf(@Nonnull final String targetIndex) throws IOException {
    final String sourceIndex = sourceIndexByTarget.get(targetIndex);
    if (sourceIndex == null) {
      throw new IOException(
          "The mirrored write log names " + targetIndex + ", which no source index maps to");
    }
    return sourceIndex;
  }

  private void reconcileAll() throws IOException {
    for (Map.Entry<String, Set<String>> perIndex : pending.entrySet()) {
      reconcile(perIndex.getKey(), new ArrayList<>(perIndex.getValue()));
    }
    pending.clear();
    pendingCount = 0;
  }

  /**
   * Makes the target's copies of {@code ids} equal to the source's: absent there, deleted here;
   * present there, written here as the source holds them. A target document whose guard is later
   * than the source's is left as it is. Every other write is conditioned on the version the target
   * held when it was looked up, so a mirrored write that lands in between wins (the cluster answers
   * 409 and the document is left alone).
   */
  private void reconcile(@Nonnull final String targetIndex, @Nonnull final List<String> ids)
      throws IOException {
    final String sourceIndex = sourceIndexOf(targetIndex);
    for (int from = 0; from < ids.size(); from += RECONCILE_BATCH) {
      final List<String> batch = ids.subList(from, Math.min(ids.size(), from + RECONCILE_BATCH));
      final Map<String, JsonNode> onTarget = held(target, targetIndex, batch, guardField != null);
      final Map<String, JsonNode> onSource = held(source, sourceIndex, batch, true);
      final StringBuilder bulk = new StringBuilder();
      for (String id : batch) {
        final JsonNode sourceDoc = onSource.get(id);
        final JsonNode targetDoc = onTarget.get(id);
        if (sourceDoc == null && targetDoc == null) {
          continue;
        }
        final ObjectNode action = target.object();
        if (sourceDoc == null) {
          asHeld(action.putObject("delete"), targetIndex, id, targetDoc);
          bulk.append(action).append('\n');
        } else if (targetDoc == null) {
          action.putObject("create").put("_index", targetIndex).put("_id", id);
          bulk.append(action).append('\n').append(sourceDoc.path("_source")).append('\n');
        } else if (targetHoldsNewer(targetDoc, sourceDoc)) {
          // The target's stamp is later than the source's. The version check below only notices a
          // write that lands during this call, so a document already newer would be overwritten.
          leftAlone++;
        } else {
          asHeld(action.putObject("index"), targetIndex, id, targetDoc);
          bulk.append(action).append('\n').append(sourceDoc.path("_source")).append('\n');
        }
      }
      if (bulk.length() == 0) {
        continue;
      }
      final JsonNode response =
          target.raw("POST", "/_bulk?refresh=true", bulk.toString(), "application/x-ndjson");
      for (JsonNode item : response.path("items")) {
        final Map.Entry<String, JsonNode> entry = item.fields().next();
        final JsonNode result = entry.getValue();
        final int status = result.path("status").asInt();
        if (status == 409) {
          // A mirrored write got there first; what it wrote is newer than anything here.
          leftAlone++;
        } else if (result.has("error")) {
          throw new IOException(
              "Reconciling "
                  + result.path("_id").asText()
                  + " on "
                  + targetIndex
                  + " failed: "
                  + result.path("error"));
        } else if ("delete".equals(entry.getKey())) {
          if ("deleted".equals(result.path("result").asText())) {
            deleted++;
          }
        } else {
          rewritten++;
        }
      }
    }
  }

  /**
   * Whether the target's guard is later than the source's, or present where the source's copy has
   * none. A missing guard, or one that is not a number, is not treated as newer.
   */
  private boolean targetHoldsNewer(
      @Nonnull final JsonNode targetDoc, @Nonnull final JsonNode sourceDoc) {
    if (guardField == null) {
      return false;
    }
    final JsonNode mine = targetDoc.path("_source").get(guardField);
    if (mine == null || !mine.isNumber()) {
      return false;
    }
    final JsonNode value = sourceDoc.path("_source").get(guardField);
    return value == null || !value.isNumber() || mine.asLong() > value.asLong();
  }

  /** The documents of {@code ids} the index holds, by id, with the version each is held at. */
  @Nonnull
  private static Map<String, JsonNode> held(
      @Nonnull final ClusterHttp cluster,
      @Nonnull final String index,
      @Nonnull final List<String> ids,
      final boolean withSource)
      throws IOException {
    final ObjectNode lookup = cluster.object();
    final ArrayNode wanted = lookup.putArray("ids");
    ids.forEach(wanted::add);
    final String endpoint = "/" + index + "/_mget" + (withSource ? "" : "?_source=false");
    final Map<String, JsonNode> found = new HashMap<>();
    for (JsonNode doc : cluster.json("POST", endpoint, lookup).path("docs")) {
      if (doc.path("found").asBoolean(false)) {
        found.put(doc.path("_id").asText(), doc);
      }
    }
    return found;
  }

  /** A bulk action on the document as the target held it, which fails with 409 if it changed. */
  private static void asHeld(
      @Nonnull final ObjectNode action,
      @Nonnull final String index,
      @Nonnull final String id,
      @Nonnull final JsonNode heldDoc) {
    action.put("_index", index).put("_id", id);
    action.put("if_seq_no", heldDoc.path("_seq_no").asLong());
    action.put("if_primary_term", heldDoc.path("_primary_term").asLong());
  }
}
