package mock;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


public class MockEntityRegistry implements EntityRegistry {
  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
    return new MockEntitySpec(entityName);
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return Collections.emptyMap();
  }
}
