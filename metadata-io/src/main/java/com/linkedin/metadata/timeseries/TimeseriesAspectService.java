package com.linkedin.metadata.timeseries;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public interface TimeseriesAspectService {

  void configure();

  void upsertDocument(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull JsonNode document);

  List<EnvelopedAspect> getAspectValues(@Nonnull final Urn urn, @Nonnull String entityName, @Nonnull String aspectName,
      @Nullable Long startTimeMillis, Long endTimeMillis, int limit);
}
