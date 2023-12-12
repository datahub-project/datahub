package mock;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import java.util.Collections;
import java.util.HashMap;
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

  @Nonnull
  @Override
  public AspectTemplateEngine getAspectTemplateEngine() {
    return new AspectTemplateEngine();
  }

  @Nonnull
  @Override
  public Map<String, AspectSpec> getAspectSpecs() {
    return new HashMap<>();
  }
}
