package io.datahubproject.metadata.context;

import com.linkedin.metadata.aspect.GraphRetriever;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RetrieverContext implements ContextInterface {
  // TODO: add AspectRetriever

  @Nonnull private final GraphRetriever graphRetriever;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
