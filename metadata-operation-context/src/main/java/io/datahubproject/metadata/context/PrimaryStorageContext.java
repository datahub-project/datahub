package io.datahubproject.metadata.context;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class PrimaryStorageContext implements ContextInterface {

  public static final PrimaryStorageContext EMPTY =
      PrimaryStorageContext.builder()
          .readPreference(ReadPreference.PRIMARY)
          .includeReadPreferenceInEntityCacheKey(false)
          .build();

  @Nonnull @Builder.Default private final ReadPreference readPreference = ReadPreference.PRIMARY;

  @Nullable private final StorageTarget storageTargetOverride;

  /**
   * When true, {@link ReadPreference} is included in {@link OperationContext#getEntityContextId()}
   * so client caches do not mix primary and replica reads. Set at deploy time when the READ pool
   * uses a distinct endpoint from PRIMARY.
   */
  @Builder.Default private final boolean includeReadPreferenceInEntityCacheKey = false;

  public PrimaryStorageContext withReadPreference(@Nonnull ReadPreference preference) {
    return toBuilder().readPreference(preference).build();
  }

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    if (!includeReadPreferenceInEntityCacheKey) {
      return Optional.empty();
    }
    int key = readPreference.hashCode();
    if (storageTargetOverride != null) {
      key = 31 * key + storageTargetOverride.hashCode();
    }
    return Optional.of(key);
  }

  @Nonnull
  public Optional<StorageTarget> getStorageTargetOverride() {
    return Optional.ofNullable(storageTargetOverride);
  }
}
