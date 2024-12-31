package io.datahubproject.openapi.v2.models;

import io.datahubproject.openapi.models.GenericAspect;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class GenericAspectV2 extends LinkedHashMap<String, Object> implements GenericAspect {

  public GenericAspectV2(Map<? extends String, ?> m) {
    super(m);
  }

  @Nonnull
  @Override
  public Map<String, Object> getValue() {
    return this;
  }

  @Nullable
  @Override
  public Map<String, Object> getSystemMetadata() {
    return null;
  }

  @Nullable
  @Override
  public Map<String, String> getHeaders() {
    return null;
  }
}
