package com.linkedin.metadata.restli;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RestliUtil {

  private RestliUtil() {
    // Utils class
  }

  /**
   * Executes the provided supplier and convert the results to a {@link Task}. Exceptions thrown
   * during the execution will be properly wrapped in {@link RestLiServiceException}.
   *
   * @param supplier The supplier to execute
   * @return A parseq {@link Task}
   */
  @Nonnull
  public static <T> Task<T> toTask(@Nonnull Supplier<T> supplier) {
    try {
      return Task.value(supplier.get());
    } catch (Throwable throwable) {

      // Convert IllegalArgumentException to BAD REQUEST
      if (throwable instanceof IllegalArgumentException
          || throwable.getCause() instanceof IllegalArgumentException) {
        throwable = badRequestException(throwable.getMessage());
      }

      if (throwable instanceof RestLiServiceException) {
        throw (RestLiServiceException) throwable;
      }

      throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, throwable);
    }
  }

  @Nonnull
  public static <T> Task<T> toTask(@Nonnull Supplier<T> supplier, String metricName) {
    Timer.Context context = MetricUtils.timer(metricName).time();
    // Stop timer on success and failure
    return toTask(supplier)
        .transform(
            orig -> {
              context.stop();
              if (orig.isFailed()) {
                MetricUtils.counter(MetricRegistry.name(metricName, "failed")).inc();
              } else {
                MetricUtils.counter(MetricRegistry.name(metricName, "success")).inc();
              }
              return orig;
            });
  }

  /**
   * Similar to {@link #toTask(Supplier)} but the supplier is expected to return an {@link Optional}
   * instead. A {@link RestLiServiceException} with 404 HTTP status code will be thrown if the
   * optional is emtpy.
   *
   * @param supplier The supplier to execute
   * @return A parseq {@link Task}
   */
  @Nonnull
  public static <T> Task<T> toTaskFromOptional(@Nonnull Supplier<Optional<T>> supplier) {
    return toTask(() -> supplier.get().orElseThrow(RestliUtil::resourceNotFoundException));
  }

  @Nonnull
  public static RestLiServiceException resourceNotFoundException() {
    return resourceNotFoundException(null);
  }

  @Nonnull
  public static RestLiServiceException resourceNotFoundException(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, message);
  }

  @Nonnull
  public static RestLiServiceException badRequestException(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, message);
  }

  @Nonnull
  public static RestLiServiceException invalidArgumentsException(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_412_PRECONDITION_FAILED, message);
  }
}
