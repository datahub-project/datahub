package io.datahubproject.metadata.context;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import java.util.Objects;
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
  @Nonnull private final CachingAspectRetriever cachingAspectRetriever;
  @Nonnull private final SearchRetriever searchRetriever;

  @Nonnull @Builder.Default
  private final EntityGraphCache entityGraphCache = EntityGraphCache.NO_OP;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }

  public static class RetrieverContextBuilder {
    private EntityGraphCache entityGraphCache = EntityGraphCache.NO_OP;

    public RetrieverContextBuilder entityGraphCache(EntityGraphCache entityGraphCache) {
      this.entityGraphCache = entityGraphCache;
      return this;
    }

    public RetrieverContext build() {
      if (this.aspectRetriever == null && this.cachingAspectRetriever != null) {
        this.aspectRetriever = this.cachingAspectRetriever;
      }

      if (this.cachingAspectRetriever == null
          && this.aspectRetriever instanceof CachingAspectRetriever) {
        this.cachingAspectRetriever = (CachingAspectRetriever) this.aspectRetriever;
      }

      return new RetrieverContext(
          Objects.requireNonNull(this.graphRetriever, "graphRetriever"),
          Objects.requireNonNull(this.aspectRetriever, "aspectRetriever"),
          Objects.requireNonNull(this.cachingAspectRetriever, "cachingAspectRetriever"),
          this.searchRetriever != null ? this.searchRetriever : SearchRetriever.EMPTY,
          this.entityGraphCache != null ? this.entityGraphCache : EntityGraphCache.NO_OP);
    }
  }

  public static final RetrieverContext EMPTY =
      RetrieverContext.builder()
          .graphRetriever(GraphRetriever.EMPTY)
          .searchRetriever(SearchRetriever.EMPTY)
          .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
          .build();
}
