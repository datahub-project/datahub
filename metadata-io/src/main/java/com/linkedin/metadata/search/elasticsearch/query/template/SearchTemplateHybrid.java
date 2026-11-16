package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Reorders document rows already in the keyword window. Does not add hits and does not change the
 * total or the facets. A moved row takes the keyword score of the position it moves into.
 */
public final class SearchTemplateHybrid {
  public static final String DOCUMENT = "document";

  private SearchTemplateHybrid() {}

  @Nonnull
  public static SearchResult rerank(
      @Nonnull SearchResult keyword,
      @Nullable SearchResult knn,
      float keywordWeight,
      float vectorWeight,
      @Nullable Float minScore,
      double keywordScale) {
    if (keyword.getEntities() == null || keyword.getEntities().isEmpty() || knn == null) {
      return keyword;
    }
    Map<String, Double> vectorScores = new HashMap<>();
    if (knn.getEntities() != null) {
      for (SearchEntity entity : knn.getEntities()) {
        if (entity.getEntity() != null && entity.getScore() != null) {
          vectorScores.put(entity.getEntity().toString(), entity.getScore());
        }
      }
    }
    List<SearchEntity> entities = new ArrayList<>(keyword.getEntities());
    List<Integer> slots = new ArrayList<>();
    List<Double> slotScores = new ArrayList<>();
    List<Scored> movable = new ArrayList<>();
    double maxKeyword = 0d;
    for (int i = 0; i < entities.size(); i++) {
      SearchEntity entity = entities.get(i);
      if (entity.getEntity() == null || !isDocument(entity)) {
        continue;
      }
      Double vector = vectorScores.get(entity.getEntity().toString());
      if (vector == null || (minScore != null && vector < minScore)) {
        continue;
      }
      double keywordScore = entity.getScore() == null ? 0d : entity.getScore();
      maxKeyword = Math.max(maxKeyword, keywordScore);
      slots.add(i);
      slotScores.add(keywordScore);
      movable.add(new Scored(entity, keywordScore, vector));
    }
    if (movable.size() < 2) {
      return keyword;
    }
    double scaleMax = maxKeyword <= 0d ? 1d : maxKeyword;
    movable.sort(
        (left, right) ->
            Double.compare(
                right.combined(keywordWeight, vectorWeight, scaleMax, keywordScale),
                left.combined(keywordWeight, vectorWeight, scaleMax, keywordScale)));
    for (int i = 0; i < slots.size(); i++) {
      SearchEntity moved = movable.get(i).entity;
      moved.setScore(slotScores.get(i));
      entities.set(slots.get(i), moved);
    }
    keyword.setEntities(new SearchEntityArray(entities));
    return keyword;
  }

  public static boolean includesDocument(@Nullable List<String> entityNames) {
    if (entityNames == null) {
      return false;
    }
    for (String name : entityNames) {
      if (DOCUMENT.equalsIgnoreCase(name)) {
        return true;
      }
    }
    return false;
  }

  public static boolean hasDocument(@Nonnull SearchResult result) {
    if (result.getEntities() == null) {
      return false;
    }
    for (SearchEntity entity : result.getEntities()) {
      if (entity.getEntity() != null && isDocument(entity)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isDocument(@Nonnull SearchEntity entity) {
    return DOCUMENT.equals(entity.getEntity().getEntityType());
  }

  private static final class Scored {
    private final SearchEntity entity;
    private final double keywordScore;
    private final double vectorScore;

    private Scored(SearchEntity entity, double keywordScore, double vectorScore) {
      this.entity = entity;
      this.keywordScore = keywordScore;
      this.vectorScore = vectorScore;
    }

    private double combined(
        float keywordWeight, float vectorWeight, double scaleMax, double keywordScale) {
      double scaled = keywordScore / scaleMax * keywordScale;
      return keywordWeight * scaled + vectorWeight * vectorScore;
    }
  }
}
