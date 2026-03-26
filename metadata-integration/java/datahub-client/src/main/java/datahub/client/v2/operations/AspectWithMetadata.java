package datahub.client.v2.operations;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Wrapper containing an aspect along with its system metadata and version.
 *
 * <p>This class is used to return both the aspect value and its version information from {@link
 * EntityClient#getAspect}. The version is extracted from SystemMetadata and used for optimistic
 * locking via the "If-Version-Match" header.
 *
 * @param <T> the aspect type
 */
@Value
public class AspectWithMetadata<T extends RecordTemplate> {
  /** The aspect value. Null if the aspect doesn't exist. */
  @Nullable T aspect;

  /** System metadata for the aspect. Null if the aspect doesn't exist or wasn't requested. */
  @Nullable SystemMetadata systemMetadata;

  /**
   * The aspect version for optimistic locking.
   *
   * <p>This is extracted from SystemMetadata:
   *
   * <ul>
   *   <li>Uses systemMetadata.systemMetadataVersion if present
   *   <li>Falls back to systemMetadata.version if systemMetadataVersion is not set
   *   <li>Returns "-1" if the aspect doesn't exist (for CREATE operations)
   * </ul>
   */
  @Nonnull String version;

  /**
   * Creates an AspectWithMetadata for a non-existent aspect.
   *
   * @param <T> the aspect type
   * @return wrapper with null aspect and version "-1"
   */
  public static <T extends RecordTemplate> AspectWithMetadata<T> nonExistent() {
    return new AspectWithMetadata<>(null, null, "-1");
  }

  /**
   * Creates an AspectWithMetadata from an aspect and its system metadata.
   *
   * @param aspect the aspect value
   * @param systemMetadata the system metadata
   * @param <T> the aspect type
   * @return wrapper with extracted version
   */
  public static <T extends RecordTemplate> AspectWithMetadata<T> from(
      @Nonnull T aspect, @Nullable SystemMetadata systemMetadata) {
    String version = extractVersion(systemMetadata);
    return new AspectWithMetadata<>(aspect, systemMetadata, version);
  }

  /**
   * Extracts the version string from SystemMetadata.
   *
   * @param systemMetadata the system metadata (may be null)
   * @return the version string, or "-1" if not available
   */
  private static String extractVersion(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata == null) {
      return "-1";
    }

    // Extract version field if present
    if (systemMetadata.hasVersion()) {
      return systemMetadata.getVersion();
    }

    // Default to "-1" if no version information
    return "-1";
  }
}
