package com.linkedin.metadata.config.usage.cigate;

import com.linkedin.metadata.config.usage.cigate.HandlerInstrumentationSurface.HandlerEntry;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Repo-specific usage registry CI gate configuration. OSS ships {@link
 * OssUsageRegistryCiGateProfile}; forks with additional API surface can provide a custom
 * implementation with extra scan roots, watched paths, and supplemental snapshot resources.
 */
public interface UsageRegistryCiGateProfile {

  /** OSS GraphQL exemption snapshot resource on the test classpath. */
  @Nonnull
  String graphqlExemptionSnapshotResource();

  /** OSS exemption snapshot resource on the test classpath. */
  @Nonnull
  String openApiExemptionSnapshotResource();

  /** OSS exemption snapshot resource on the test classpath. */
  @Nonnull
  String restLiExemptionSnapshotResource();

  /**
   * Optional supplemental GraphQL exemptions merged into {@link
   * #graphqlExemptionSnapshotResource()}.
   */
  @Nullable
  String supplementalGraphqlExemptionSnapshotResource();

  /**
   * Optional supplemental OpenAPI exemptions merged into {@link
   * #openApiExemptionSnapshotResource()}.
   */
  @Nullable
  String supplementalOpenApiExemptionSnapshotResource();

  /**
   * Optional supplemental Rest.li exemptions merged into {@link
   * #restLiExemptionSnapshotResource()}.
   */
  @Nullable
  String supplementalRestLiExemptionSnapshotResource();

  /** Repo-relative OpenAPI controller source roots scanned for {@code buildOpenapi(...)} usage. */
  @Nonnull
  List<String> openApiSourceRootSuffixes();

  /** Repo-relative Rest.li resource source roots scanned for {@code buildRestli(...)} usage. */
  @Nonnull
  List<String> restLiSourceRootSuffixes();

  /** Repo-relative GraphQL schema directories scanned for server root fields. */
  @Nonnull
  List<String> graphqlSchemaDirSuffixes();

  /** Repo-relative GraphQL client directories scanned for named operations. */
  @Nonnull
  List<String> graphqlClientDirSuffixes();

  /** Git path prefixes that trigger the OpenAPI instrumentation CI gate. */
  @Nonnull
  List<String> openApiWatchedPrefixSuffixes();

  /** Git path prefixes that trigger the Rest.li instrumentation CI gate. */
  @Nonnull
  List<String> restLiWatchedPrefixSuffixes();

  /** Git path prefixes that trigger the GraphQL usage surface CI gate. */
  @Nonnull
  List<String> graphqlWatchedPrefixSuffixes();

  /**
   * Repo-relative exact paths whose changes also trigger the GraphQL gate (exemption snapshots,
   * {@code usage_operations.yaml}, etc.).
   */
  @Nonnull
  List<String> graphqlWatchedExactPaths();

  /**
   * Repo-relative exact paths whose changes also trigger the OpenAPI gate (exemption snapshots,
   * etc.).
   */
  @Nonnull
  List<String> openApiWatchedExactPaths();

  /**
   * Repo-relative exact paths whose changes also trigger the Rest.li gate (exemption snapshots,
   * etc.).
   */
  @Nonnull
  List<String> restLiWatchedExactPaths();

  /**
   * Fork-specific handlers exempt from usage instrumentation (prefer exemption snapshot entries).
   */
  default boolean isOpenApiHandlerExempt(@Nonnull HandlerEntry handler) {
    return false;
  }

  /**
   * Fork-specific Rest.li handlers exempt from usage instrumentation (prefer exemption snapshot
   * entries).
   */
  default boolean isRestLiHandlerExempt(@Nonnull HandlerEntry handler) {
    return false;
  }
}
