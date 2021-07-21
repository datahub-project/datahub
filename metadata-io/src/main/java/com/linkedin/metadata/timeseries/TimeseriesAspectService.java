package com.linkedin.metadata.timeseries;

import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Nonnull;


public interface TimeseriesAspectService {

  void configure();

  void upsertDocument(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull JsonNode document);
}
