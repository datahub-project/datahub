package com.linkedin.metadata.systemmetadata.cache;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class KeyAspectEntityCountCacheKey implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String ALL_TYPES = "ALL";

  @Nonnull private final String searchContextId;
  @Nonnull private final String entityTypesKey;

  @Nonnull
  public static KeyAspectEntityCountCacheKey of(
      @Nonnull String searchContextId, @Nonnull List<String> sortedEntityTypes) {
    if (sortedEntityTypes.isEmpty()) {
      return new KeyAspectEntityCountCacheKey(searchContextId, ALL_TYPES);
    }
    return new KeyAspectEntityCountCacheKey(searchContextId, String.join(",", sortedEntityTypes));
  }
}
