package datahub.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;

public class MetadataResponseFuture implements Future<MetadataWriteResponse> {
  private final Future<SimpleHttpResponse> requestFuture;
  private final AtomicReference<MetadataWriteResponse> responseReference;
  private final CountDownLatch responseLatch;
  private final ResponseMapper mapper;

  public MetadataResponseFuture(
      Future<SimpleHttpResponse> underlyingFuture,
      AtomicReference<MetadataWriteResponse> responseAtomicReference,
      CountDownLatch responseLatch) {
    this.requestFuture = underlyingFuture;
    this.responseReference = responseAtomicReference;
    this.responseLatch = responseLatch;
    this.mapper = null;
  }

  public MetadataResponseFuture(
      Future<SimpleHttpResponse> underlyingFuture, ResponseMapper mapper) {
    this.requestFuture = underlyingFuture;
    this.responseReference = null;
    this.responseLatch = null;
    this.mapper = mapper;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return requestFuture.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return requestFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return requestFuture.isDone();
  }

  @SneakyThrows
  @Override
  public MetadataWriteResponse get() throws InterruptedException, ExecutionException {
    SimpleHttpResponse response = requestFuture.get();
    if (mapper != null) {
      return mapper.map(response);
    } else {
      // We wait for the callback to fill this out
      responseLatch.await();
      return responseReference.get();
    }
  }

  @Override
  public MetadataWriteResponse get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    SimpleHttpResponse response = requestFuture.get(timeout, unit);
    if (mapper != null) {
      return mapper.map(response);
    } else {
      // We wait for the callback to fill this out
      responseLatch.await(timeout, unit);
      return responseReference.get();
    }
  }

  @FunctionalInterface
  public interface ResponseMapper {
    MetadataWriteResponse map(SimpleHttpResponse httpResponse);
  }
}
