package com.linkedin.metadata.config.usage.cigate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlExemptionSnapshot;
import java.io.InputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class UsageRegistrySnapshotLoader {

  private UsageRegistrySnapshotLoader() {}

  @Nonnull
  static GraphqlExemptionSnapshot loadGraphqlExemptions(
      @Nonnull ClassLoader classLoader,
      @Nonnull ObjectMapper mapper,
      @Nonnull UsageRegistryCiGateProfile profile)
      throws Exception {
    GraphqlExemptionSnapshot baseline =
        loadOptional(
            classLoader,
            mapper,
            profile.graphqlExemptionSnapshotResource(),
            GraphqlExemptionSnapshot.class);
    if (baseline == null) {
      baseline = GraphqlExemptionSnapshot.empty();
    }
    String supplemental = profile.supplementalGraphqlExemptionSnapshotResource();
    if (supplemental == null || supplemental.isBlank()) {
      return baseline;
    }
    GraphqlExemptionSnapshot overlay =
        loadRequired(classLoader, mapper, supplemental, GraphqlExemptionSnapshot.class);
    return baseline.merge(overlay);
  }

  @Nonnull
  static HandlerExemptionSnapshot loadOpenApiExemptions(
      @Nonnull ClassLoader classLoader,
      @Nonnull ObjectMapper mapper,
      @Nonnull UsageRegistryCiGateProfile profile)
      throws Exception {
    return loadExemptions(
        classLoader,
        mapper,
        profile.openApiExemptionSnapshotResource(),
        profile.supplementalOpenApiExemptionSnapshotResource());
  }

  @Nonnull
  static HandlerExemptionSnapshot loadRestLiExemptions(
      @Nonnull ClassLoader classLoader,
      @Nonnull ObjectMapper mapper,
      @Nonnull UsageRegistryCiGateProfile profile)
      throws Exception {
    return loadExemptions(
        classLoader,
        mapper,
        profile.restLiExemptionSnapshotResource(),
        profile.supplementalRestLiExemptionSnapshotResource());
  }

  @Nonnull
  private static HandlerExemptionSnapshot loadExemptions(
      @Nonnull ClassLoader classLoader,
      @Nonnull ObjectMapper mapper,
      @Nonnull String resource,
      @Nullable String supplementalResource)
      throws Exception {
    HandlerExemptionSnapshot baseline =
        loadOptional(classLoader, mapper, resource, HandlerExemptionSnapshot.class);
    if (baseline == null) {
      baseline = HandlerExemptionSnapshot.empty();
    }
    if (supplementalResource == null || supplementalResource.isBlank()) {
      return baseline;
    }
    HandlerExemptionSnapshot overlay =
        loadRequired(classLoader, mapper, supplementalResource, HandlerExemptionSnapshot.class);
    return baseline.merge(overlay);
  }

  @Nonnull
  private static <T> T loadRequired(
      @Nonnull ClassLoader classLoader,
      @Nonnull ObjectMapper mapper,
      @Nonnull String resource,
      @Nonnull Class<T> type)
      throws Exception {
    T value = loadOptional(classLoader, mapper, resource, type);
    if (value == null) {
      throw new IllegalStateException("Missing snapshot resource: " + resource);
    }
    return value;
  }

  @Nullable
  private static <T> T loadOptional(
      @Nonnull ClassLoader classLoader,
      @Nonnull ObjectMapper mapper,
      @Nonnull String resource,
      @Nonnull Class<T> type)
      throws Exception {
    try (InputStream in = classLoader.getResourceAsStream(resource)) {
      if (in == null) {
        return null;
      }
      return mapper.readValue(in, type);
    }
  }
}
