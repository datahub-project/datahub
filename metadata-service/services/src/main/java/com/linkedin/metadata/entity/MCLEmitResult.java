package com.linkedin.metadata.entity;

import com.linkedin.mxe.MetadataChangeLog;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Result of attempting to emit a Metadata Change Log (MCL) to Kafka.
 *
 * <p>This class uses a state pattern to represent three distinct outcomes:
 *
 * <ol>
 *   <li><b>Not Emitted</b>: MCL was intentionally not sent (filtered, CDC mode, etc.)
 *   <li><b>Emitted & Pending</b>: MCL was sent to Kafka, async production in progress
 *   <li><b>Emitted & Resolved</b>: MCL production completed (successfully or with failure)
 * </ol>
 *
 * <p>Use factory methods to construct instances in valid states:
 *
 * <ul>
 *   <li>{@link #notEmitted(MetadataChangeLog, boolean)} - MCL was filtered/skipped
 *   <li>{@link #emitted(MetadataChangeLog, Future, boolean)} - MCL sent to Kafka
 * </ul>
 */
@Getter
@ToString
@EqualsAndHashCode
public final class MCLEmitResult {

  @Nonnull private final MetadataChangeLog metadataChangeLog;

  private final Future<?> mclFuture;

  /** Whether the MCL was preprocessed (e.g., synchronous index update triggered). */
  private final boolean processedMCL;

  /** Whether this MCL was emitted to Kafka (regardless of production outcome). */
  private final boolean emitted;

  private MCLEmitResult(
      @Nonnull MetadataChangeLog metadataChangeLog,
      Future<?> mclFuture,
      boolean processedMCL,
      boolean emitted) {
    this.metadataChangeLog = metadataChangeLog;
    this.mclFuture = mclFuture;
    this.processedMCL = processedMCL;
    this.emitted = emitted;

    // Invariant: emitted=true implies mclFuture != null
    if (emitted && mclFuture == null) {
      throw new IllegalArgumentException(
          "Invalid state: emitted=true but mclFuture is null. "
              + "Use notEmitted() factory method for non-emitted MCLs.");
    }

    // Invariant: emitted=false implies mclFuture == null
    if (!emitted && mclFuture != null) {
      throw new IllegalArgumentException(
          "Invalid state: emitted=false but mclFuture is not null. "
              + "Use emitted() factory method for emitted MCLs.");
    }
  }

  /**
   * Creates a result for an MCL that was intentionally not emitted to Kafka.
   *
   * @param metadataChangeLog the MCL that was not emitted
   * @param processedMCL whether the MCL was preprocessed (e.g., sync index update)
   * @return result indicating the MCL was not emitted
   */
  @Nonnull
  public static MCLEmitResult notEmitted(
      @Nonnull MetadataChangeLog metadataChangeLog, boolean processedMCL) {
    return new MCLEmitResult(metadataChangeLog, null, processedMCL, false);
  }

  /**
   * Creates a result for an MCL that was emitted to Kafka.
   *
   * @param metadataChangeLog the MCL that was emitted
   * @param mclFuture the future representing the async Kafka send operation
   * @param processedMCL whether the MCL was preprocessed (e.g., sync index update)
   * @return result indicating the MCL was emitted
   * @throws IllegalArgumentException if mclFuture is null
   */
  @Nonnull
  public static MCLEmitResult emitted(
      @Nonnull MetadataChangeLog metadataChangeLog,
      @Nonnull Future<?> mclFuture,
      boolean processedMCL) {
    return new MCLEmitResult(metadataChangeLog, mclFuture, processedMCL, true);
  }

  /**
   * Gets the production result, providing detailed information about success or failure.
   *
   * <p>For non-emitted MCLs, returns {@link ProductionResult#notEmitted()}. For emitted MCLs,
   * blocks until the Kafka send completes and returns the result.
   *
   * @return the production result with success/failure details
   */
  @Nonnull
  public ProductionResult getProductionResult() {
    if (!emitted) {
      return ProductionResult.notEmitted();
    }

    try {
      mclFuture.get();
      return ProductionResult.success();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return ProductionResult.failure(e);
    } catch (ExecutionException e) {
      // Unwrap the cause - ExecutionException is just boilerplate wrapper
      Throwable cause = e.getCause();
      return ProductionResult.failure(cause != null ? cause : e);
    }
  }

  /**
   * Attempts to determine if the MCL was successfully produced to Kafka.
   *
   * <p>This method blocks until the Kafka send operation completes or fails. For non-emitted MCLs,
   * this always returns {@code false}.
   *
   * @return {@code true} if the MCL was successfully written to Kafka, {@code false} otherwise
   */
  public boolean isProduced() {
    return getProductionResult().isSuccess();
  }

  /** Represents the outcome of attempting to produce an MCL to Kafka. */
  @Getter
  @ToString
  @EqualsAndHashCode
  public static final class ProductionResult {
    private final ProductionStatus status;
    private final Throwable error;

    private ProductionResult(ProductionStatus status, Throwable error) {
      this.status = status;
      this.error = error;
    }

    @Nonnull
    public static ProductionResult notEmitted() {
      return new ProductionResult(ProductionStatus.NOT_EMITTED, null);
    }

    @Nonnull
    public static ProductionResult success() {
      return new ProductionResult(ProductionStatus.SUCCESS, null);
    }

    @Nonnull
    public static ProductionResult failure(@Nonnull Throwable error) {
      return new ProductionResult(ProductionStatus.FAILURE, error);
    }

    public boolean isSuccess() {
      return status == ProductionStatus.SUCCESS;
    }

    public boolean isFailure() {
      return status == ProductionStatus.FAILURE;
    }

    public boolean wasNotEmitted() {
      return status == ProductionStatus.NOT_EMITTED;
    }

    @Nonnull
    public Optional<Throwable> getError() {
      return Optional.ofNullable(error);
    }
  }

  public enum ProductionStatus {
    NOT_EMITTED,
    SUCCESS,
    FAILURE
  }
}
