package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * The convention for naming search indices
 */
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
}
