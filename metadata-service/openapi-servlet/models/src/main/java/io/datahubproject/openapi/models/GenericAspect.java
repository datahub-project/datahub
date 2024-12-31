package io.datahubproject.openapi.models;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface GenericAspect {
  @Nonnull
  Map<String, Object> getValue();

  @Nullable
  Map<String, Object> getSystemMetadata();

  @Nullable
  Map<String, String> getHeaders();
}
