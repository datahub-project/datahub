package io.datahubproject.metadata.context;

import com.linkedin.common.urn.Urn;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ViewAuthorizationContext implements ContextInterface {

  /**
   * Graphql has a lot of redundant `canView` authorization checks, to reduce the repeated checks
   * for view authorization, maintain a list of urns that have already been identified as viewable
   * for the request.
   */
  @Nonnull @Builder.Default private Set<Urn> viewableUrns = ConcurrentHashMap.newKeySet();

  public boolean canView(@Nonnull Collection<Urn> urns) {
    if (urns.isEmpty()) {
      return false;
    }
    return viewableUrns.containsAll(urns);
  }

  public void addViewableUrns(@Nonnull Collection<Urn> urns) {
    viewableUrns.addAll(urns);
  }

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
