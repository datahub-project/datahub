package com.linkedin.metadata.testing;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import javax.annotation.Nonnull;


/**
 * Utilities related to URNs for testing.
 */
public final class Urns {
  private Urns() {
  }

  @Nonnull
  public static CorpuserUrn makeCorpUserUrn(@Nonnull String name) {
    return new CorpuserUrn(name);
  }

  @Nonnull
  public static CorpGroupUrn makeCorpGroupUrn(@Nonnull String name) {
    return new CorpGroupUrn(name);
  }

  @Nonnull
  public static DatasetUrn makeDatasetUrn(@Nonnull String name) {
    return new DatasetUrn(new DataPlatformUrn("mysql"), name, FabricType.DEV);
  }

  @Nonnull
  public static DatasetUrn makeDatasetUrn(@Nonnull String platform, @Nonnull String name, @Nonnull FabricType fabricType) {
    return new DatasetUrn(new DataPlatformUrn(platform), name, fabricType);
  }

  @Nonnull
  public static DataProcessUrn makeDataProcessUrn(@Nonnull String name) {
    return new DataProcessUrn("Azure Data Factory", name, FabricType.DEV);
  }
}
