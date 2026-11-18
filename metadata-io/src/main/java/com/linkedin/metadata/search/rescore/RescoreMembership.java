package com.linkedin.metadata.search.rescore;

import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps a reranked page a permutation of the extracted hits plus any supplementary additions.
 * Duplicates are dropped. A hit the pipeline dropped is appended in extracted order.
 */
@Slf4j
public final class RescoreMembership {
  private RescoreMembership() {}

  public static void reconcile(
      @Nonnull final SearchResult result, @Nonnull final List<SearchEntity> extractedEntities) {
    final List<SearchEntity> current = result.getEntities();
    if (current == null) {
      return;
    }

    final Map<String, SearchEntity> byUrn = new LinkedHashMap<>();
    for (SearchEntity entity : current) {
      if (entity.getEntity() != null) {
        byUrn.putIfAbsent(entity.getEntity().toString(), entity);
      }
    }
    final int duplicatesRemoved = current.size() - byUrn.size();

    int dropsRestored = 0;
    for (SearchEntity entity : extractedEntities) {
      if (entity.getEntity() != null && !byUrn.containsKey(entity.getEntity().toString())) {
        byUrn.put(entity.getEntity().toString(), entity);
        dropsRestored++;
      }
    }

    if (duplicatesRemoved > 0 || dropsRestored > 0) {
      log.warn(
          "V2.5 rescore membership repaired: removed {} duplicate result(s), restored {} dropped "
              + "result(s) (extracted={}, pipeline={})",
          duplicatesRemoved,
          dropsRestored,
          extractedEntities.size(),
          current.size());
      final SearchEntityArray reconciled = new SearchEntityArray();
      reconciled.addAll(byUrn.values());
      result.setEntities(reconciled);
    }
  }
}
