package io.datahubproject.metadata.context;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RetrieverContext
    implements ContextInterface, com.linkedin.metadata.aspect.RetrieverContext {

  @Nonnull private final GraphRetriever graphRetriever;
  @Nonnull private final AspectRetriever aspectRetriever;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
