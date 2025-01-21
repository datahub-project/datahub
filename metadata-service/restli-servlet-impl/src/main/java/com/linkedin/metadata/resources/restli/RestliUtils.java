package com.linkedin.metadata.resources.restli;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.metadata.dao.throttle.APIThrottleException;
import com.linkedin.metadata.restli.NonExceptionHttpErrorResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.exception.ActorAccessException;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RestliUtils {

  private RestliUtils() {
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

      final RestLiServiceException finalException;

      // Convert IllegalArgumentException to BAD REQUEST
      if (throwable instanceof IllegalArgumentException
          || throwable.getCause() instanceof IllegalArgumentException) {
        finalException = badRequestException(throwable.getMessage());
      } else if (throwable.getCause() instanceof ActorAccessException) {
          finalException = forbidden(throwable.getCause().getMessage());
      } else if (throwable instanceof APIThrottleException) {
        finalException = apiThrottled(throwable.getMessage());
      } else if (throwable instanceof RestLiServiceException) {
        finalException = (RestLiServiceException) throwable;
      } else {
        finalException = new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, throwable);
      }

      throw finalException;
    }
  }

  @Nonnull
  public static <T> Task<T> toTask(@Nonnull OperationContext opContext, @Nonnull Supplier<T> supplier, String metricName) {
    return opContext.withSpan(metricName, () -> {
      // Stop timer on success and failure
      return toTask(supplier)
              .transform(
                      orig -> {
                        if (orig.isFailed()) {
                          MetricUtils.counter(MetricRegistry.name(metricName, "failed")).inc();
                        } else {
                          MetricUtils.counter(MetricRegistry.name(metricName, "success")).inc();
                        }
                        return orig;
                      });
    }, MetricUtils.DROPWIZARD_METRIC, "true");
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
    return toTask(() -> supplier.get().orElseThrow(RestliUtils::resourceNotFoundException));
  }

  @Nonnull
  public static RestLiServiceException resourceNotFoundException() {
    return resourceNotFoundException(null);
  }

  @Nonnull
  public static RestLiServiceException nonExceptionResourceNotFound() {
    return new NonExceptionHttpErrorResponse(HttpStatus.S_404_NOT_FOUND);
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

  @Nonnull
  public static RestLiServiceException apiThrottled(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_429_TOO_MANY_REQUESTS, message);
  }

  @Nonnull
  public static RestLiServiceException forbidden(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_403_FORBIDDEN, message);
  }
}
