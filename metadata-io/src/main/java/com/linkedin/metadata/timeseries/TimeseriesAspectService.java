package com.linkedin.metadata.timeseries;

import javax.annotation.Nonnull;


public interface TimeseriesAspectService {

  void configure();

  void upsertDocument(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull String document);
}
