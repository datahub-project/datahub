package datahub.client.v2.exceptions;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nullable;

/**
 * Base unchecked exception for all DataHub Java SDK V2 errors.
 *
 * <p>Provides rich context about what operation failed, including entity URN, aspect name, and
 * operation type. Preserves the cause chain for debugging.
 *
 * <p>This is an unchecked exception (extends RuntimeException) to maintain fluent API ergonomics
 * and align with patterns from popular Java SDKs (AWS SDK, Google Cloud SDK, Stripe SDK).
 *
 * <p>Subclasses provide specific exception types for different failure scenarios, enabling users to
 * implement appropriate retry logic and error handling strategies.
 */
public class DataHubClientException extends RuntimeException {

  @Nullable private final Urn entityUrn;
  @Nullable private final String aspectName;
  @Nullable private final String operation;

  /**
   * Constructs a DataHubClientException with a message and cause.
   *
   * @param message error message describing what failed
   * @param cause the underlying exception that caused this failure
   */
  public DataHubClientException(String message, Throwable cause) {
    this(message, cause, null, null, null);
  }

  /**
   * Constructs a DataHubClientException with a message only.
   *
   * @param message error message describing what failed
   */
  public DataHubClientException(String message) {
    this(message, null, null, null, null);
  }

  /**
   * Constructs a DataHubClientException with full context.
   *
   * @param message error message describing what failed
   * @param cause the underlying exception that caused this failure
   * @param entityUrn the URN of the entity involved in the operation
   * @param aspectName the name of the aspect involved in the operation
   * @param operation the operation that failed (e.g., "fetch", "save", "parse")
   */
  public DataHubClientException(
      String message,
      @Nullable Throwable cause,
      @Nullable Urn entityUrn,
      @Nullable String aspectName,
      @Nullable String operation) {
    super(message, cause);
    this.entityUrn = entityUrn;
    this.aspectName = aspectName;
    this.operation = operation;
  }

  /**
   * Returns the URN of the entity involved in the failed operation.
   *
   * @return entity URN, or null if not applicable
   */
  @Nullable
  public Urn getEntityUrn() {
    return entityUrn;
  }

  /**
   * Returns the name of the aspect involved in the failed operation.
   *
   * @return aspect name, or null if not applicable
   */
  @Nullable
  public String getAspectName() {
    return aspectName;
  }

  /**
   * Returns the operation that failed.
   *
   * @return operation name (e.g., "fetch", "save", "parse"), or null if not specified
   */
  @Nullable
  public String getOperation() {
    return operation;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    if (entityUrn != null) {
      sb.append(" [entityUrn=").append(entityUrn).append("]");
    }
    if (aspectName != null) {
      sb.append(" [aspectName=").append(aspectName).append("]");
    }
    if (operation != null) {
      sb.append(" [operation=").append(operation).append("]");
    }
    return sb.toString();
  }
}
