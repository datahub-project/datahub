package com.linkedin.metadata.entity;

import javax.annotation.Nonnull;

public interface AspectMigrationsDao {

  @Nonnull
  Iterable<String> listAllUrns(final int start, final int pageSize);

  long countEntities();

  boolean checkIfAspectExists(@Nonnull final String aspectName);
}
