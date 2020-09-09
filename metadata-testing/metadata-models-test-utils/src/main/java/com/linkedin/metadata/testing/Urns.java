package com.linkedin.metadata.testing;

import com.linkedin.common.FabricType;
import com.linkedin.common.RegisteredSchemaType;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetGroupUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.FeatureUrn;
import com.linkedin.common.urn.MetricUrn;
import com.linkedin.common.urn.RegisteredSchemaUrn;
import com.linkedin.metadata.urn.InternalDataPlatformUrn;
import com.linkedin.metadata.urn.InternalDatasetUrn;
import com.linkedin.metadata.urn.InternalRegisteredSchemaUrn;
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
    return new InternalDatasetUrn(new InternalDataPlatformUrn("mysql"), name, FabricType.DEV);
  }

  @Nonnull
  public static DatasetUrn makeDatasetUrn(@Nonnull String platform, @Nonnull String name, @Nonnull FabricType fabricType) {
    return new InternalDatasetUrn(new InternalDataPlatformUrn(platform), name, fabricType);
  }

  @Nonnull
  public static MetricUrn makeMetricUrn(@Nonnull String name) {
    return new MetricUrn("UMP", name);
  }

  @Nonnull
  public static DatasetGroupUrn makeDatasetGroupUrn(@Nonnull String name) {
    return new DatasetGroupUrn("foo", name);
  }

  @Nonnull
  public static RegisteredSchemaUrn makeRegisteredSchemaUrn(@Nonnull String name) {
    return new InternalRegisteredSchemaUrn(RegisteredSchemaType.KAFKA, name);
  }

  @Nonnull
  public static FeatureUrn makeFeatureUrn(@Nonnull String name) {
    return new FeatureUrn("foo", name);
  }
}
