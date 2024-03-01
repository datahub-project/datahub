package io.datahubproject.metadata.context;

import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class AuthorizerContext implements ContextInterface {

  public static final AuthorizerContext EMPTY =
      AuthorizerContext.builder().authorizer(Authorizer.EMPTY).build();

  @Nonnull private final Authorizer authorizer;

  /**
   * No need to consider the authorizer in the cache context since it is ultimately determined by
   * the underlying search context
   *
   * @return
   */
  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
