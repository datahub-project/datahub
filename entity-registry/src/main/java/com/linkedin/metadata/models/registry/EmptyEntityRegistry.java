package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EmptyEntityRegistry implements EntityRegistry {
  public static final EntityRegistry EMPTY = new EmptyEntityRegistry();

  @Nullable
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
    return null;
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
  public Map<String, AspectSpec> getAspectSpecs() {
    return Collections.emptyMap();
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return Collections.emptyMap();
  }

  @Nonnull
  @Override
  public AspectTemplateEngine getAspectTemplateEngine() {
    return new AspectTemplateEngine();
  }
}
