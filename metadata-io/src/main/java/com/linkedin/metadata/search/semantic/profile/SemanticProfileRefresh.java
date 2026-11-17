package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Pure updates to profile current flags and the entity refresh marker. */
public final class SemanticProfileRefresh {

  /** Separates the aspect name from the watched path in a refresh marker key. */
  public static final char WATCH_KEY_SEPARATOR = '#';

  private SemanticProfileRefresh() {}

  /**
   * The {@code refreshMarker.sources} key for one watched path. Profiles watching different paths
   * of one aspect keep separate clocks, so a change to one path does not mark the other stale.
   */
  @Nonnull
  public static String watchKey(@Nonnull String aspectName, @Nonnull String path) {
    return aspectName + WATCH_KEY_SEPARATOR + canonicalPath(path);
  }

  /**
   * Every marker key a binding reads or writes. A watch of the whole aspect (no paths) keeps the
   * bare aspect name.
   */
  @Nonnull
  public static Set<String> watchKeys(@Nonnull ContentBinding binding) {
    Set<String> keys = new HashSet<>();
    binding
        .aspectFields()
        .forEach(
            (aspect, paths) -> {
              if (paths.isEmpty()) {
                keys.add(aspect);
              } else {
                paths.forEach(path -> keys.add(watchKey(aspect, path)));
              }
            });
    return keys;
  }

  /**
   * Marker keys of the watches on {@code aspect} whose selected values differ between the two
   * versions. Empty when the binding does not watch the aspect.
   */
  @Nonnull
  public static Set<String> changedWatchKeys(
      @Nonnull ContentBinding binding,
      @Nonnull String aspect,
      @Nullable DataMap before,
      @Nullable DataMap after) {
    Set<String> watched = binding.aspectFields().get(aspect);
    if (watched == null) {
      return Set.of();
    }
    if (watched.isEmpty()) {
      return Objects.equals(before, after) ? Set.of() : Set.of(aspect);
    }
    Set<String> keys = new HashSet<>();
    for (String path : changedPaths(before, after, watched)) {
      keys.add(watchKey(aspect, path));
    }
    return keys;
  }

  /**
   * Keeps only the marker entries for {@code watchKeys}. A profile records just the clocks its
   * binding reads, so a sibling binding's clock never marks it stale.
   */
  @Nonnull
  public static SemanticRefreshMarker restrict(
      @Nonnull SemanticRefreshMarker marker, @Nonnull Set<String> watchKeys) {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    marker
        .getSources()
        .forEach(
            (key, token) -> {
              if (watchKeys.contains(key)) {
                sources.put(key, token);
              }
            });
    return new SemanticRefreshMarker().setSources(sources);
  }

  /** Orders tokens by creation time, then version. */
  public static int compare(@Nonnull SemanticSourceToken left, @Nonnull SemanticSourceToken right) {
    int byCreated = Long.compare(left.getCreatedTime(), right.getCreatedTime());
    return byCreated != 0 ? byCreated : Long.compare(left.getVersion(), right.getVersion());
  }

  @Nonnull
  public static SemanticSourceToken later(
      @Nonnull SemanticSourceToken left, @Nonnull SemanticSourceToken right) {
    return compare(left, right) >= 0 ? left : right;
  }

  @Nonnull
  private static String canonicalPath(@Nonnull String path) {
    String body = path.startsWith("$") ? path.substring(1) : path;
    return body.startsWith(".") ? body.substring(1) : body;
  }

  /**
   * Watched paths whose selected values differ. An empty watch set reports {@code *} when the maps
   * differ. A path's matches are keyed by location, so map iteration order is not a change.
   */
  @Nonnull
  public static Set<String> changedPaths(
      @Nullable DataMap before, @Nullable DataMap after, @Nonnull Set<String> watched) {
    if (watched.isEmpty()) {
      return Objects.equals(before, after) ? Set.of() : Set.of("*");
    }
    Set<String> changed = new java.util.HashSet<>();
    for (String path : watched) {
      if (!valuesAt(before, path).equals(valuesAt(after, path))) {
        changed.add(path);
      }
    }
    return changed;
  }

  /** Rejects a path the child grammar does not accept. */
  public static void checkPath(@Nonnull String path) {
    parse(path);
  }

  /**
   * True when {@code represented} is at least as new as every bound source the entity marker has
   * recorded. A missing entry means no watched-field change has been recorded, so that source is
   * covered.
   */
  public static boolean coversBoundSources(
      @Nonnull SemanticRefreshMarker represented,
      @Nullable SemanticContent stored,
      @Nonnull Set<String> boundWatchKeys) {
    for (String key : boundWatchKeys) {
      SemanticSourceToken storedAt = markerToken(stored, key);
      if (storedAt == null) {
        continue;
      }
      SemanticSourceToken representedAt = represented.getSources().get(key);
      if (representedAt == null || compare(representedAt, storedAt) < 0) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private static SemanticSourceToken markerToken(
      @Nullable SemanticContent stored, @Nonnull String watchKey) {
    if (stored == null || !stored.hasRefreshMarker()) {
      return null;
    }
    return stored.getRefreshMarker().getSources().get(watchKey);
  }

  @Nonnull
  private static Map<String, Object> valuesAt(@Nullable Object root, @Nonnull String path) {
    Map<String, Object> found = new LinkedHashMap<>();
    collect(root, "", parse(path), 0, found);
    return found;
  }

  private static void collect(
      @Nullable Object current,
      @Nonnull String location,
      @Nonnull List<Segment> segments,
      int index,
      @Nonnull Map<String, Object> found) {
    if (current == null) {
      return;
    }
    if (index == segments.size()) {
      found.put(location, current);
      return;
    }
    Segment segment = segments.get(index);
    switch (segment.kind) {
      case FIELD -> {
        if (current instanceof DataMap map && map.containsKey(segment.name)) {
          collect(map.get(segment.name), child(location, segment.name), segments, index + 1, found);
        }
      }
      case INDEX -> {
        if (current instanceof DataList list && segment.index < list.size()) {
          collect(
              list.get(segment.index),
              location + "[" + segment.index + "]",
              segments,
              index + 1,
              found);
        }
      }
      case WILDCARD -> {
        if (current instanceof DataList list) {
          for (int i = 0; i < list.size(); i++) {
            collect(list.get(i), location + "[" + i + "]", segments, index + 1, found);
          }
        } else if (current instanceof DataMap map) {
          for (String key : map.keySet()) {
            collect(map.get(key), child(location, key), segments, index + 1, found);
          }
        }
      }
      default -> throw new IllegalStateException("Unknown path segment");
    }
  }

  @Nonnull
  private static String child(@Nonnull String location, @Nonnull String name) {
    if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return location.isEmpty() ? name : location + "." + name;
    }
    return location + "['" + name.replace("\\", "\\\\").replace("'", "\\'") + "']";
  }

  @Nonnull
  private static List<Segment> parse(@Nonnull String path) {
    String body = path;
    if (body.startsWith("$")) {
      body = body.substring(1);
    }
    if (body.startsWith(".")) {
      body = body.substring(1);
    }
    if (body.isEmpty() || body.startsWith(".")) {
      throw new IllegalArgumentException("Unsupported watch path: " + path);
    }
    List<Segment> segments = new ArrayList<>();
    int cursor = 0;
    while (cursor < body.length()) {
      char character = body.charAt(cursor);
      if (character == '.' || (segments.isEmpty() && isNameStart(character))) {
        if (character == '.') {
          if (cursor + 1 < body.length() && body.charAt(cursor + 1) == '.') {
            throw new IllegalArgumentException("Unsupported watch path: " + path);
          }
          cursor++;
        }
        int start = cursor;
        if (cursor >= body.length() || !isNameStart(body.charAt(cursor))) {
          throw new IllegalArgumentException("Unsupported watch path: " + path);
        }
        while (cursor < body.length() && isNameChar(body.charAt(cursor))) {
          cursor++;
        }
        segments.add(new Segment(Kind.FIELD, body.substring(start, cursor), -1));
      } else if (character == '[') {
        cursor++;
        if (cursor >= body.length()) {
          throw new IllegalArgumentException("Unsupported watch path: " + path);
        }
        char bracket = body.charAt(cursor);
        if (bracket == '*') {
          cursor++;
          cursor = closingBracket(body, cursor, path);
          segments.add(new Segment(Kind.WILDCARD, "", -1));
        } else if (bracket == '\'' || bracket == '"') {
          Quote quote = quoted(body, cursor, path);
          cursor = closingBracket(body, quote.next, path);
          segments.add(new Segment(Kind.FIELD, quote.name, -1));
        } else if (Character.isDigit(bracket)) {
          int start = cursor;
          while (cursor < body.length() && Character.isDigit(body.charAt(cursor))) {
            cursor++;
          }
          if (cursor < body.length()
              && (body.charAt(cursor) == ':' || body.charAt(cursor) == ',')) {
            throw new IllegalArgumentException("Unsupported watch path: " + path);
          }
          cursor = closingBracket(body, cursor, path);
          segments.add(
              new Segment(Kind.INDEX, "", Integer.parseInt(body.substring(start, cursor - 1))));
        } else {
          throw new IllegalArgumentException("Unsupported watch path: " + path);
        }
      } else {
        throw new IllegalArgumentException("Unsupported watch path: " + path);
      }
    }
    if (segments.isEmpty()) {
      throw new IllegalArgumentException("Unsupported watch path: " + path);
    }
    return segments;
  }

  private static int closingBracket(@Nonnull String body, int cursor, @Nonnull String path) {
    if (cursor >= body.length() || body.charAt(cursor) != ']') {
      throw new IllegalArgumentException("Unsupported watch path: " + path);
    }
    return cursor + 1;
  }

  @Nonnull
  private static Quote quoted(@Nonnull String body, int openingQuote, @Nonnull String path) {
    char quote = body.charAt(openingQuote);
    StringBuilder name = new StringBuilder();
    for (int cursor = openingQuote + 1; cursor < body.length(); cursor++) {
      char character = body.charAt(cursor);
      if (character == '\\') {
        if (cursor + 1 >= body.length()) {
          throw new IllegalArgumentException("Unsupported watch path: " + path);
        }
        name.append(body.charAt(cursor + 1));
        cursor++;
      } else if (character == quote) {
        return new Quote(name.toString(), cursor + 1);
      } else {
        name.append(character);
      }
    }
    throw new IllegalArgumentException("Unsupported watch path: " + path);
  }

  private static boolean isNameStart(char character) {
    return character == '_' || Character.isLetter(character);
  }

  private static boolean isNameChar(char character) {
    return isNameStart(character) || Character.isDigit(character);
  }

  private enum Kind {
    FIELD,
    INDEX,
    WILDCARD
  }

  private record Segment(Kind kind, String name, int index) {}

  private record Quote(String name, int next) {}
}
