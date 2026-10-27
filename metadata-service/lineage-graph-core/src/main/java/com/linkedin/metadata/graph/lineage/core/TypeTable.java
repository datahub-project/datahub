package com.linkedin.metadata.graph.lineage.core;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Small interned name table (entity types, relationship types, platforms, environments) with a
 * fixed id limit so ids fit in a few bits of a packed field. Type names ignore case, as {@code
 * LineageRegistry} does; facet values (platforms, environments) are exact, as GMS compares them.
 *
 * <p>The single writer may add names while walks read: ids never change and a name is readable
 * before its id is handed out.
 */
public final class TypeTable {

  private final String kind;
  private final int limit;
  private final boolean ignoreCase;
  private volatile String[] names = new String[0];
  private final ConcurrentHashMap<String, Integer> ids = new ConcurrentHashMap<>();

  public TypeTable(String kind, int limit) {
    this(kind, limit, true);
  }

  public TypeTable(String kind, int limit, boolean ignoreCase) {
    this.kind = kind;
    this.limit = limit;
    this.ignoreCase = ignoreCase;
  }

  private String key(String name) {
    return ignoreCase ? name.toLowerCase(Locale.ROOT) : name;
  }

  public synchronized int intern(String name) {
    String key = key(name);
    Integer id = ids.get(key);
    if (id != null) {
      return id;
    }
    String[] current = names;
    if (current.length >= limit) {
      throw new IllegalStateException("More than " + limit + " distinct " + kind + " values");
    }
    String[] next = Arrays.copyOf(current, current.length + 1);
    next[current.length] = name;
    names = next;
    ids.put(key, current.length);
    return current.length;
  }

  /** Returns the id of {@code name}, or -1. */
  public int find(String name) {
    Integer id = ids.get(key(name));
    return id == null ? -1 : id;
  }

  public String name(int id) {
    return names[id];
  }

  public int size() {
    return names.length;
  }

  public List<String> names() {
    return List.of(names);
  }

  /** A table holding {@code names} in order, for loading a snapshot. */
  static TypeTable of(String kind, int limit, boolean ignoreCase, List<String> names) {
    TypeTable t = new TypeTable(kind, limit, ignoreCase);
    names.forEach(t::intern);
    return t;
  }
}
