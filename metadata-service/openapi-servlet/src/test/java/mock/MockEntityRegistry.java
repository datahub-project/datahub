package mock;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class MockEntityRegistry implements EntityRegistry {
  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
    return new MockEntitySpec(entityName);
  }

  @Nullable
  @Override
  public EventSpec getEventSpec(@Nonnull String eventName) {
    return null;
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return Collections.emptyMap();
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return null;
  }
}
