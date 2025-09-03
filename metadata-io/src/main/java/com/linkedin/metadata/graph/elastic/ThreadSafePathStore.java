package com.linkedin.metadata.graph.elastic;

import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe wrapper for storing paths during lineage computation. Uses ConcurrentHashMap with
 * Set<UrnArray> for efficient duplicate detection.
 */
public class ThreadSafePathStore {
  protected final ConcurrentHashMap<Urn, Set<UrnArray>> pathMap = new ConcurrentHashMap<>();

  public void addPath(Urn destinationUrn, UrnArray path) {
    pathMap.compute(
        destinationUrn,
        (key, existingSet) -> {
          if (existingSet == null) {
            Set<UrnArray> newSet = ConcurrentHashMap.newKeySet();
            newSet.add(path);
            return newSet;
          } else {
            existingSet.add(path);
            return existingSet;
          }
        });
  }

  public Set<UrnArray> getPaths(Urn destinationUrn) {
    return pathMap.getOrDefault(destinationUrn, ConcurrentHashMap.newKeySet());
  }

  public Map<Urn, UrnArrayArray> toUrnArrayArrayMap() {
    Map<Urn, UrnArrayArray> result = new HashMap<>();
    pathMap.forEach(
        (urn, pathSet) -> {
          UrnArrayArray urnArrayArray = new UrnArrayArray(pathSet.size());
          urnArrayArray.addAll(pathSet);
          result.put(urn, urnArrayArray);
        });
    return result;
  }
}
