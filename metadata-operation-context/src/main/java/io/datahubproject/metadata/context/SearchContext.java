package io.datahubproject.metadata.context;

import com.linkedin.common.UrnArray;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class SearchContext implements ContextInterface {

  public static SearchContext EMPTY =
      SearchContext.builder()
          .indexConvention(IndexConventionImpl.noPrefix("", new EntityIndexConfiguration()))
          .searchableFieldTypes(Collections.emptyMap())
          .searchableFieldPaths(Collections.emptyMap())
          .build();

  public static SearchContext withFlagDefaults(
      @Nonnull SearchContext searchContext,
      @Nonnull Function<SearchFlags, SearchFlags> flagDefaults) {
    try {
      return searchContext.toBuilder()
          // update search flags
          .searchFlags(flagDefaults.apply(searchContext.getSearchFlags().copy()))
          .build();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public static SearchContext withLineageFlagDefaults(
      @Nonnull SearchContext searchContext,
      @Nonnull Function<LineageFlags, LineageFlags> flagDefaults) {
    try {
      return searchContext.toBuilder()
          .lineageFlags(flagDefaults.apply(searchContext.getLineageFlags().copy()))
          .build();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(
          "Unable to clone RecordTemplate: " + searchContext.getLineageFlags(), e);
    }
  }

  @Nonnull private final IndexConvention indexConvention;

  /**
   * Process-wide cluster lookup. Excluded from equals and cache keys: connection identity is not
   * part of a search result. Null on {@link #EMPTY} and on tests that do not talk to Elasticsearch.
   */
  @EqualsAndHashCode.Exclude @Nullable private final SearchClusterAccess searchClusterAccess;

  /**
   * Process-wide template runtime. Excluded from equals and cache keys: it selects how a request is
   * built, not the stored result. Null on {@link #EMPTY} and when templates are off.
   */
  @EqualsAndHashCode.Exclude @Nullable private final SearchTemplateRuntime searchTemplateRuntime;

  @Nonnull private final SearchFlags searchFlags;
  @Nonnull private final LineageFlags lineageFlags;
  @Nullable private final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes;
  @Nullable private final Map<PathSpec, String> searchableFieldPaths;

  /**
   * Resolved once at SearchContext construction from config. Null means callers should treat as
   * empty / resolve on demand (tests / EMPTY context).
   */
  @Nullable private final List<String> defaultSearchEntityNames;

  @Nullable private final List<String> defaultAutocompleteEntityNames;

  @Nullable private final List<String> defaultBrowseEntityNames;

  @Nullable private final List<String> prioritizedSourceEntityTypes;

  @Nullable private final List<String> prioritizedDatahubEntityTypes;

  /**
   * Search V3 template surface ({@code searchEntities}, {@code scrollEntities}, ...). Selection
   * reads this. It is not an API operation name and not itself a cache component; the resolved
   * template URN is.
   */
  @Nullable private final String searchSurface;

  /**
   * Template chosen for this search invocation. Selection does not read it. Each invocation
   * overwrites it. Included in {@link #getCacheKeyComponent()} by URN and parent-chain {@code
   * lastModified}.
   */
  @Nullable private final ResolvedSearchTemplate resolvedSearchTemplate;

  /**
   * Internal index-occupancy checks do not run a search template. Absent from equality and the
   * search-result cache key because it selects the request builder, not the stored result.
   */
  @EqualsAndHashCode.Exclude private final boolean skipTemplateExecution;

  public boolean isRestrictedSearch() {
    return Optional.ofNullable(searchFlags.isIncludeRestricted()).orElse(false);
  }

  public SearchContext withFlagDefaults(Function<SearchFlags, SearchFlags> flagDefaults) {
    return SearchContext.withFlagDefaults(this, flagDefaults);
  }

  public SearchContext withLineageFlagDefaults(Function<LineageFlags, LineageFlags> flagDefaults) {
    return SearchContext.withLineageFlagDefaults(this, flagDefaults);
  }

  /**
   * Currently relying on the consistent hashing of String
   *
   * @return
   */
  @Override
  public Optional<Integer> getCacheKeyComponent() {
    // The index-name prefix is intentionally NOT part of this key. It is resolved per operation
    // (see IndexPrefixResolver) and this context carries no OperationFingerprint to resolve it.
    // Here the prefix is a static deploy-wide constant (contributes nothing distinguishing); a
    // deployment that scopes the prefix per operation MUST fold that discriminator into the
    // OperationContext-level cache key via its enrichment, not here.
    // Absent resolution contributes 0, so templates that are off do not move existing cache keys.
    String templateKey =
        resolvedSearchTemplate == null ? "" : resolvedSearchTemplate.cacheKeyComponent();
    return Optional.of(
        Stream.of(keySearchFlags().toString(), keyLineageFlags(), templateKey)
            .mapToInt(String::hashCode)
            .sum());
  }

  /**
   * Only certain flags change the cache key
   *
   * @return
   */
  private SearchFlags keySearchFlags() {
    try {
      // whether cache is enabled or not does not impact the result
      SearchFlags keyFlags = searchFlags.clone().setSkipCache(false);
      keyFlags.data().remove("searchTemplate");
      return keyFlags;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  // Converts LineageFlags into a consistent string for usage in the context key
  private String keyLineageFlags() {
    String startTime =
        lineageFlags.getStartTimeMillis() != null
            ? lineageFlags.getStartTimeMillis().toString()
            : "";
    String endTime =
        lineageFlags.getEndTimeMillis() != null ? lineageFlags.getEndTimeMillis().toString() : "";
    String perHopLimit =
        lineageFlags.getEntitiesExploredPerHopLimit() != null
            ? lineageFlags.getEntitiesExploredPerHopLimit().toString()
            : "";

    // Map's iterator does not result in a consistent string so we need to convert to something that
    // will be consistent
    // based on a sort order
    String ignoreAsHops;
    if (lineageFlags.getIgnoreAsHops() != null) {
      ignoreAsHops =
          lineageFlags.getIgnoreAsHops().entrySet().stream()
              .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
              .sorted(
                  Comparator.comparing((Pair<String, UrnArray> pair) -> pair.getFirst())
                      .thenComparing(pair -> Optional.ofNullable(pair.getSecond()).toString()))
              .collect(Collectors.toList())
              .toString();

    } else {
      ignoreAsHops = "";
    }

    return "{startTime="
        + startTime
        + ",endTime="
        + endTime
        + "perHopLimit="
        + perHopLimit
        + "ignoreAsHops="
        + ignoreAsHops
        + "}";
  }

  public static class SearchContextBuilder {

    public SearchContextBuilder searchFlags(@Nullable SearchFlags searchFlags) {
      this.searchFlags = searchFlags != null ? searchFlags : buildDefaultSearchFlags();
      return this;
    }

    public SearchContextBuilder lineageFlags(@Nullable LineageFlags lineageFlags) {
      this.lineageFlags = lineageFlags != null ? lineageFlags : buildDefaultLineageFlags();
      return this;
    }

    public SearchContext build() {
      if (this.searchFlags == null) {
        searchFlags(buildDefaultSearchFlags());
      }
      if (this.lineageFlags == null) {
        lineageFlags(buildDefaultLineageFlags());
      }
      return new SearchContext(
          this.indexConvention,
          this.searchClusterAccess,
          this.searchTemplateRuntime,
          this.searchFlags,
          this.lineageFlags,
          this.searchableFieldTypes,
          this.searchableFieldPaths,
          this.defaultSearchEntityNames,
          this.defaultAutocompleteEntityNames,
          this.defaultBrowseEntityNames,
          this.prioritizedSourceEntityTypes,
          this.prioritizedDatahubEntityTypes,
          this.searchSurface,
          this.resolvedSearchTemplate,
          this.skipTemplateExecution);
    }
  }

  /**
   * Cluster lookup for this operation. Production system context always stamps one; tests that
   * execute Elasticsearch must use {@link SearchClusterAccess#fixed}.
   */
  @Nonnull
  public SearchClusterAccess requireSearchClusterAccess() {
    if (searchClusterAccess == null) {
      throw new IllegalStateException(
          "SearchClusterAccess is not set on SearchContext; stamp SearchClusterAccess.fixed(client) "
              + "in tests or the cluster registry in production");
    }
    return searchClusterAccess;
  }

  private static SearchFlags buildDefaultSearchFlags() {
    return new SearchFlags().setSkipCache(false);
  }

  private static LineageFlags buildDefaultLineageFlags() {
    return new LineageFlags();
  }
}
