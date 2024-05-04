package datahub.client.rest;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Arrays;
import javax.net.ssl.SSLException;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.util.TimeValue;

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
}
