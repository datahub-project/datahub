package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.util.Pair;
import java.util.Optional;
import javax.annotation.Nonnull;

/** The convention for naming search indices */
public interface IndexConvention {
  Optional<String> getPrefix();

  @Nonnull
  String getIndexName(Class<? extends RecordTemplate> documentClass);

  @Nonnull
  String getIndexName(EntitySpec entitySpec);

  @Nonnull
  String getIndexName(String baseIndexName);

  @Nonnull
  String getEntityIndexName(String entityName);

  @Nonnull
  String getTimeseriesAspectIndexName(String entityName, String aspectName);

  @Nonnull
  String getAllEntityIndicesPattern();

  @Nonnull
  String getAllTimeseriesAspectIndicesPattern();

  /**
   * Inverse of getEntityIndexName
   *
   * @param indexName The index name to parse
   * @return a string, the entity name that that index is for, or empty if one cannot be extracted
   */
  Optional<String> getEntityName(String indexName);

  /**
   * Inverse of getEntityIndexName
   *
   * @param timeseriesAspectIndexName The index name to parse
   * @return a pair of strings, the entity name and the aspect name that that index is for, or empty
   *     if one cannot be extracted
   */
  Optional<Pair<String, String>> getEntityAndAspectName(String timeseriesAspectIndexName);
}
