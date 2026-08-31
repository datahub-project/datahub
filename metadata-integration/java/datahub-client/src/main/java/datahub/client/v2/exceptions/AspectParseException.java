package datahub.client.v2.exceptions;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Exception thrown when parsing or deserializing an aspect response fails.
 *
 * <p>This typically indicates data format issues like:
 *
 * <ul>
 *   <li>Invalid JSON in server response
 *   <li>Schema mismatch between server and client
 *   <li>Unexpected data structure
 *   <li>Deserialization errors
 * </ul>
 *
 * <p><b>Retry strategy:</b> NOT retryable - indicates a data format or compatibility issue that
 * won't be fixed by retrying.
 */
public class AspectParseException extends DataHubClientException {

  /**
   * Constructs an AspectParseException with full context.
   *
   * @param entityUrn the URN of the entity whose aspect couldn't be parsed
   * @param aspectName the name of the aspect that couldn't be parsed
   * @param cause the underlying exception that caused the parse failure
   */
  public AspectParseException(
      @Nonnull Urn entityUrn, @Nonnull String aspectName, @Nullable Throwable cause) {
    super(
        String.format("Failed to parse aspect %s for entity %s", aspectName, entityUrn),
        cause,
        entityUrn,
        aspectName,
        "parse");
  }

  /**
   * Constructs an AspectParseException with a custom message.
   *
   * @param message custom error message
   * @param entityUrn the URN of the entity whose aspect couldn't be parsed
   * @param aspectName the name of the aspect that couldn't be parsed
   * @param cause the underlying exception that caused the parse failure
   */
  public AspectParseException(
      @Nonnull String message,
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nullable Throwable cause) {
    super(message, cause, entityUrn, aspectName, "parse");
  }
}
