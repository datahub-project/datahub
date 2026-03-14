package io.datahubproject.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/** Immutable allow list for topic names, supporting exact match and prefix matching with '*'. */
public class TopicAllowList {

  private final Set<String> exactEntries;
  private final List<String> prefixEntries;

  /**
   * @param config comma-delimited topic patterns. Entries ending with {@code *} use prefix
   *     matching; others use exact match.
   */
  public TopicAllowList(@Nonnull String config) {
    Set<String> exact = new HashSet<>();
    List<String> prefixes = new ArrayList<>();
    for (String raw : config.split(",")) {
      String entry = raw.trim();
      if (entry.isEmpty()) {
        continue;
      }
      if (entry.endsWith("*")) {
        prefixes.add(entry.substring(0, entry.length() - 1));
      } else {
        exact.add(entry);
      }
    }
    this.exactEntries = Collections.unmodifiableSet(exact);
    this.prefixEntries = Collections.unmodifiableList(prefixes);
  }

  /** Returns {@code true} if the topic is allowed by either an exact or prefix entry. */
  public boolean isAllowed(@Nonnull String topic) {
    if (exactEntries.contains(topic)) {
      return true;
    }
    for (String prefix : prefixEntries) {
      if (topic.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
