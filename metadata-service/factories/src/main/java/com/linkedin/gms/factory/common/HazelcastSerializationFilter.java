package com.linkedin.gms.factory.common;

import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.linkedin.metadata.config.hazelcast.HazelcastSerializationAllowListContributor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Restricts which classes the embedded Hazelcast member will Java-deserialize.
 *
 * <p>Members read {@code Serializable} / {@code Externalizable} values from peers on the cluster
 * port. Without a filter Hazelcast resolves any class on the classpath, so a peer (or anything that
 * can reach the port) can trigger the {@code readObject} of any library type, including copies of
 * Guava bundled inside other jars that cannot be upgraded independently.
 *
 * <p>Hazelcast's defaults stay enabled: they allow {@code java*}, {@code com.hazelcast.} and
 * arrays, and the entries below add what DataHub stores. Types with a Hazelcast {@code
 * StreamSerializer} or zero-config Compact serialization (lineage cache keys) do not pass through
 * this filter.
 *
 * <p>This is defense in depth, not access control. Anything that can reach the cluster port can
 * still join as a member and write to the maps, and payloads made only of JDK types (large arrays,
 * deeply nested collections) are not limited by class name. Keep the port on a private network.
 */
public final class HazelcastSerializationFilter {

  /**
   * DataHub's own Serializable cache values and keys (search and lineage caches). No dependency on
   * the GMS classpath is shaded into these packages; a relocation into them would pass this filter
   * unchecked.
   */
  static final Set<String> DEFAULT_PACKAGE_PREFIXES =
      Set.of("com.linkedin.metadata.", "org.javatuples.");

  /**
   * Always rejected, even when a contributor or operator allows a broader prefix. These are the
   * Guava copies on the GMS classpath; Guava's collection {@code readObject} methods have a history
   * of allocation DoS issues and DataHub never stores Guava types in Hazelcast.
   */
  static final Set<String> BLOCKED_PACKAGE_PREFIXES =
      Set.of(
          "com.google.common.",
          "graphql.com.google.common.",
          "org.apache.iceberg.relocated.com.google.common.");

  private HazelcastSerializationFilter() {}

  /**
   * Installs the filter on {@code config}.
   *
   * @param contributors allow-list entries from other modules
   * @param extraClassNames comma-separated class names from configuration, may be blank
   * @param extraPackagePrefixes comma-separated prefixes from configuration, may be blank
   */
  public static void apply(
      @Nonnull Config config,
      @Nonnull Collection<HazelcastSerializationAllowListContributor> contributors,
      @Nullable String extraClassNames,
      @Nullable String extraPackagePrefixes) {
    config
        .getSerializationConfig()
        .setJavaSerializationFilterConfig(
            build(contributors, extraClassNames, extraPackagePrefixes));
  }

  @Nonnull
  static JavaSerializationFilterConfig build(
      @Nonnull Collection<HazelcastSerializationAllowListContributor> contributors,
      @Nullable String extraClassNames,
      @Nullable String extraPackagePrefixes) {
    Set<String> classNames = new TreeSet<>();
    Set<String> prefixes = new TreeSet<>(DEFAULT_PACKAGE_PREFIXES);
    for (HazelcastSerializationAllowListContributor contributor : contributors) {
      classNames.addAll(contributor.classNames());
      prefixes.addAll(contributor.packagePrefixes());
    }
    classNames.addAll(splitCsv(extraClassNames));
    prefixes.addAll(splitCsv(extraPackagePrefixes));

    return new JavaSerializationFilterConfig()
        .setDefaultsDisabled(false)
        .setWhitelist(new ClassFilter().setClasses(classNames).setPrefixes(prefixes))
        .setBlacklist(new ClassFilter().setPrefixes(BLOCKED_PACKAGE_PREFIXES));
  }

  @Nonnull
  private static Set<String> splitCsv(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return Set.of();
    }
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }
}
