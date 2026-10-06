package com.linkedin.metadata.config.usage.cigate;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** OSS usage registry CI gate defaults. */
public final class OssUsageRegistryCiGateProfile implements UsageRegistryCiGateProfile {

  static final UsageRegistryCiGateProfile INSTANCE = new OssUsageRegistryCiGateProfile();

  static final String CONFIGURATION_TEST_RESOURCES =
      "metadata-service/configuration/src/test/resources/";
  static final String USAGE_OPERATIONS_YAML =
      "metadata-service/configuration/src/main/resources/usage_operations.yaml";

  private OssUsageRegistryCiGateProfile() {}

  @Override
  @Nonnull
  public String graphqlExemptionSnapshotResource() {
    return "graphql_usage_exemptions.snapshot.yaml";
  }

  @Override
  @Nonnull
  public String openApiExemptionSnapshotResource() {
    return "openapi_usage_exemptions.snapshot.yaml";
  }

  @Override
  @Nonnull
  public String restLiExemptionSnapshotResource() {
    return "restli_usage_exemptions.snapshot.yaml";
  }

  @Override
  @Nullable
  public String supplementalGraphqlExemptionSnapshotResource() {
    return null;
  }

  @Override
  @Nullable
  public String supplementalOpenApiExemptionSnapshotResource() {
    return null;
  }

  @Override
  @Nullable
  public String supplementalRestLiExemptionSnapshotResource() {
    return null;
  }

  @Override
  @Nonnull
  public List<String> openApiSourceRootSuffixes() {
    return List.of(
        "metadata-service/openapi-servlet/src/main/java",
        "metadata-service/openapi-entity-servlet/src/main/java",
        "metadata-service/openapi-analytics-servlet/src/main/java",
        "metadata-service/openapi-metadatatests-servlet/src/main/java",
        "metadata-service/auth-servlet-impl/src/main/java",
        "metadata-service/iceberg-catalog/src/main/java");
  }

  @Override
  @Nonnull
  public List<String> restLiSourceRootSuffixes() {
    return List.of("metadata-service/restli-servlet-impl/src/main/java");
  }

  @Override
  @Nonnull
  public List<String> graphqlSchemaDirSuffixes() {
    return List.of("datahub-graphql-core/src/main/resources");
  }

  @Override
  @Nonnull
  public List<String> graphqlClientDirSuffixes() {
    return List.of("datahub-web-react/src");
  }

  @Override
  @Nonnull
  public List<String> openApiWatchedPrefixSuffixes() {
    return openApiSourceRootSuffixes().stream().map(suffix -> suffix + "/").toList();
  }

  @Override
  @Nonnull
  public List<String> restLiWatchedPrefixSuffixes() {
    return restLiSourceRootSuffixes().stream().map(suffix -> suffix + "/").toList();
  }

  @Override
  @Nonnull
  public List<String> graphqlWatchedPrefixSuffixes() {
    List<String> prefixes = new java.util.ArrayList<>();
    for (String suffix : graphqlSchemaDirSuffixes()) {
      prefixes.add(suffix.endsWith("/") ? suffix : suffix + "/");
    }
    for (String suffix : graphqlClientDirSuffixes()) {
      prefixes.add(suffix.endsWith("/") ? suffix : suffix + "/");
    }
    return List.copyOf(prefixes);
  }

  @Override
  @Nonnull
  public List<String> graphqlWatchedExactPaths() {
    return List.of(
        exemptionSnapshotPath(graphqlExemptionSnapshotResource()), USAGE_OPERATIONS_YAML);
  }

  @Override
  @Nonnull
  public List<String> openApiWatchedExactPaths() {
    return List.of(exemptionSnapshotPath(openApiExemptionSnapshotResource()));
  }

  @Override
  @Nonnull
  public List<String> restLiWatchedExactPaths() {
    return List.of(exemptionSnapshotPath(restLiExemptionSnapshotResource()));
  }

  @Nonnull
  private static String exemptionSnapshotPath(@Nonnull String resourceName) {
    return CONFIGURATION_TEST_RESOURCES + resourceName;
  }
}
