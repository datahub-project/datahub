package com.linkedin.metadata.search.elasticsearch.query.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Write time of one search-template aspect, taken from its system metadata. */
final class SearchTemplateModified {
  private SearchTemplateModified() {}

  static long at(@Nullable SystemAspect aspect) {
    if (aspect == null) {
      return 0L;
    }
    long metadata = at(aspect.getSystemMetadata());
    if (metadata > 0L) {
      return metadata;
    }
    Timestamp createdOn = aspect.getCreatedOn();
    return createdOn == null ? 0L : createdOn.getTime();
  }

  static long at(@Nullable SystemMetadata metadata) {
    if (metadata == null) {
      return 0L;
    }
    if (metadata.hasAspectModified()) {
      long modified = time(metadata.getAspectModified());
      if (modified > 0L) {
        return modified;
      }
    }
    if (metadata.hasAspectCreated()) {
      return time(metadata.getAspectCreated());
    }
    return 0L;
  }

  static long at(@Nullable JsonNode metadata) {
    if (metadata == null || metadata.isMissingNode() || metadata.isNull()) {
      return 0L;
    }
    long modified = time(metadata.path("aspectModified"));
    if (modified > 0L) {
      return modified;
    }
    return time(metadata.path("aspectCreated"));
  }

  private static long time(@Nullable AuditStamp stamp) {
    if (stamp == null || !stamp.hasTime()) {
      return 0L;
    }
    return stamp.getTime();
  }

  private static long time(@Nonnull JsonNode stamp) {
    JsonNode time = stamp.path("time");
    if (time.isNumber()) {
      return time.asLong();
    }
    if (!time.isTextual()) {
      return 0L;
    }
    try {
      return Long.parseLong(time.asText());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }
}
