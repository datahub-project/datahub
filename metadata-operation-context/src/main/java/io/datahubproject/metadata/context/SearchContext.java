package io.datahubproject.metadata.context;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
public class SearchContext implements ContextInterface {

  public static SearchContext EMPTY =
      SearchContext.builder().indexConvention(IndexConventionImpl.NO_PREFIX).build();

  public static SearchContext withFlagDefaults(
      @Nonnull SearchContext searchContext,
      @Nonnull Function<SearchFlags, SearchFlags> flagDefaults) {
    return searchContext.toBuilder()
        // update search flags
        .searchFlags(flagDefaults.apply(searchContext.getSearchFlags()))
        .build();
  }

  @Nonnull private final IndexConvention indexConvention;
  @Nonnull private final SearchFlags searchFlags;

  public boolean isRestrictedSearch() {
    return Optional.ofNullable(searchFlags.isIncludeRestricted()).orElse(false);
  }

  public SearchContext withFlagDefaults(Function<SearchFlags, SearchFlags> flagDefaults) {
    return SearchContext.withFlagDefaults(this, flagDefaults);
  }

  /**
   * Currently relying on the consistent hashing of String
   *
   * @return
   */
  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.of(
        Stream.of(indexConvention.getPrefix().orElse(""), keySearchFlags().toString())
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

  public static class SearchContextBuilder {

    public SearchContextBuilder searchFlags(@Nullable SearchFlags searchFlags) {
      this.searchFlags = searchFlags != null ? searchFlags : buildDefaultSearchFlags();
      return this;
    }

    public SearchContext build() {
      if (this.searchFlags == null) {
        searchFlags(buildDefaultSearchFlags());
      }
      return new SearchContext(this.indexConvention, this.searchFlags);
    }
  }

  private static SearchFlags buildDefaultSearchFlags() {
    return new SearchFlags().setSkipCache(false);
  }
}
