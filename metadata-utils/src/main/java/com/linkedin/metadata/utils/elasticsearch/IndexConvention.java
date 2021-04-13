package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * The convention for naming search indices
 */
public interface IndexConvention {
  @Nonnull
  String getIndexName(Class<? extends RecordTemplate> documentClass);
}
