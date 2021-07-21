package com.linkedin.metadata.temporal;

import javax.annotation.Nonnull;


public interface TemporalAspectService {

  void configure();

  void upsertDocument(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull String document);
}
