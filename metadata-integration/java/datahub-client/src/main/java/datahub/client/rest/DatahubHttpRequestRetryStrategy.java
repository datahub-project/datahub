package datahub.client.rest;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Arrays;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;

@Slf4j
public class DatahubHttpRequestRetryStrategy extends DefaultHttpRequestRetryStrategy {
  public DatahubHttpRequestRetryStrategy() {
    this(1, TimeValue.ofSeconds(10));
  }

  public DatahubHttpRequestRetryStrategy(int maxRetries, TimeValue retryInterval) {
    super(
        maxRetries,
        retryInterval,
        Arrays.asList(
            InterruptedIOException.class,
            UnknownHostException.class,
            ConnectException.class,
            ConnectionClosedException.class,
            NoRouteToHostException.class,
            SSLException.class),
        Arrays.asList(
            HttpStatus.SC_TOO_MANY_REQUESTS,
            HttpStatus.SC_SERVICE_UNAVAILABLE,
            HttpStatus.SC_INTERNAL_SERVER_ERROR));
  }

  @Override
  public boolean retryRequest(
      HttpRequest request, IOException exception, int execCount, HttpContext context) {
    log.warn("Checking if retry is needed: {}", execCount);
    return super.retryRequest(request, exception, execCount, context);
  }

  @Override
  public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
    return super.retryRequest(response, execCount, context);
  }
}
