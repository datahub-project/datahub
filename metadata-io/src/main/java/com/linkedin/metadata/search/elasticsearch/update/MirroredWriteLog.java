package com.linkedin.metadata.search.elasticsearch.update;

import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Appends every mirrored delete and by-query operation to a small index on the target cluster while
 * a family's history is being copied. A copy partition can read a document, see it deleted on both
 * clusters by the mirror, and then create it on the target; replaying this log after the copy
 * undoes such resurrections. Entries are plain documents: the query is stored as a JSON string and
 * never indexed. The replay reconciles every document a logged operation could have touched against
 * the source, so an update's script is not recorded; the source's version of the document replaces
 * whatever the update produced.
 *
 * <p>The log is {@linkplain #setActive active} until the process sees the family's migration row
 * verified: from then on nothing will replay the entries, so recording them would only grow an
 * index the next system update removes. The row's periodic poll keeps the flag current, and turns
 * the log back on if the copy is ever planned again.
 */
public final class MirroredWriteLog {
  public static final String FIELD_TS = "ts";
  public static final String FIELD_KIND = "kind";
  public static final String FIELD_INDEX = "index";
  public static final String FIELD_ID = "id";
  public static final String FIELD_QUERY = "query";

  public enum Kind {
    DELETE_BY_ID,
    DELETE_BY_QUERY,
    UPDATE_BY_QUERY
  }

  private final ESBulkProcessor target;
  private final MigrationIndexNames names;
  private volatile boolean active = true;

  public MirroredWriteLog(
      @Nonnull final ESBulkProcessor target, @Nonnull final MigrationIndexNames names) {
    this.target = target;
    this.names = names;
  }

  /** Whether entries are recorded; false once the copy that would replay them has verified. */
  public boolean isActive() {
    return active;
  }

  public void setActive(final boolean active) {
    this.active = active;
  }

  public void record(
      @Nonnull final OperationContext opContext,
      @Nonnull final Kind kind,
      @Nonnull final String targetIndex,
      @Nullable final String id,
      @Nullable final QueryBuilder query) {
    if (!active) {
      return;
    }
    final long now = System.currentTimeMillis();
    final Map<String, Object> document = new LinkedHashMap<>();
    document.put(FIELD_TS, now);
    document.put(FIELD_KIND, kind.name());
    document.put(FIELD_INDEX, targetIndex);
    if (id != null) {
      document.put(FIELD_ID, id);
    }
    if (query != null) {
      document.put(FIELD_QUERY, toJson(query));
    }
    // Route by target index so one family's log entries keep their order on one bulk thread.
    target.add(
        opContext, targetIndex, new IndexRequest(names.logIndex(opContext)).source(document));
  }

  @Nonnull
  static String toJson(@Nonnull final ToXContent content) {
    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
      if (content.isFragment()) {
        builder.startObject();
        content.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
      } else {
        content.toXContent(builder, ToXContent.EMPTY_PARAMS);
      }
      return builder.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize a mirrored write log entry", e);
    }
  }
}
