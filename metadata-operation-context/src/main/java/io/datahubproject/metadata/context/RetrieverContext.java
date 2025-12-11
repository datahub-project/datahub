/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.metadata.context;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
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

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }

  public static class RetrieverContextBuilder {
    public RetrieverContext build() {
      if (this.aspectRetriever == null && this.cachingAspectRetriever != null) {
        this.aspectRetriever = this.cachingAspectRetriever;
      }

      if (this.cachingAspectRetriever == null
          && this.aspectRetriever instanceof CachingAspectRetriever) {
        this.cachingAspectRetriever = (CachingAspectRetriever) this.aspectRetriever;
      }

      return new RetrieverContext(
          this.graphRetriever,
          Objects.requireNonNull(this.aspectRetriever),
          Objects.requireNonNull(this.cachingAspectRetriever),
          this.searchRetriever);
    }
  }

  public static final RetrieverContext EMPTY =
      RetrieverContext.builder()
          .graphRetriever(GraphRetriever.EMPTY)
          .searchRetriever(SearchRetriever.EMPTY)
          .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
          .build();
}
