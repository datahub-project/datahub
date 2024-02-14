package io.datahubproject.metadata.context;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
public class SearchContext implements ContextInterface {

  @Nonnull private final IndexConvention indexConvention;
  @Nullable private final SearchFlags searchFlags;

  public boolean isRestrictedSearch() {
    return Optional.ofNullable(searchFlags).map(SearchFlags::isIncludeRestricted).orElse(false);
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
                Optional.ofNullable(searchFlags).map(RecordTemplate::toString).orElse(""))
            .mapToInt(String::hashCode)
            .sum());
  }
}
