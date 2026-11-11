package io.datahubproject.test.search;

import java.util.List;
import javax.annotation.Nonnull;

/** One hit the scorers and a later judge can read without querying the index again. */
public final class RankedSearchHit {
  private final String urn;
  private final String entityType;
  private final String name;
  private final String description;
  private final double score;
  private final List<String> matchedFields;

  public RankedSearchHit(
      @Nonnull String urn,
      @Nonnull String entityType,
      @Nonnull String name,
      @Nonnull String description,
      double score,
      @Nonnull List<String> matchedFields) {
    this.urn = urn;
    this.entityType = entityType;
    this.name = name;
    this.description = description;
    this.score = score;
    this.matchedFields = List.copyOf(matchedFields);
  }

  @Nonnull
  public String getUrn() {
    return urn;
  }

  @Nonnull
  public String getEntityType() {
    return entityType;
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Nonnull
  public String getDescription() {
    return description;
  }

  public double getScore() {
    return score;
  }

  @Nonnull
  public List<String> getMatchedFields() {
    return matchedFields;
  }
}
