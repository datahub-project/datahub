package io.datahubproject.metadata.context;

import com.linkedin.common.UrnArray;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.util.Pair;
import java.util.Comparator;
import java.util.Optional;
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
      SearchContext.builder().indexConvention(IndexConventionImpl.noPrefix("")).build();

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
  @Nonnull private final SearchFlags searchFlags;
  @Nonnull private final LineageFlags lineageFlags;

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
    return Optional.of(
        Stream.of(
                indexConvention.getPrefix().orElse(""),
                keySearchFlags().toString(),
                keyLineageFlags())
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
      return searchFlags.clone().setSkipCache(false);
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
      return new SearchContext(this.indexConvention, this.searchFlags, this.lineageFlags);
    }
  }

  private static SearchFlags buildDefaultSearchFlags() {
    return new SearchFlags().setSkipCache(false);
  }

  private static LineageFlags buildDefaultLineageFlags() {
    return new LineageFlags();
  }
}
