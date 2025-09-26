package com.linkedin.r2.transport.http.server;

import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import java.time.Duration;
import lombok.Getter;

public class RAPJakartaServlet extends AbstractJakartaR2Servlet {
  private static final long serialVersionUID = 0L;

  @Getter private final HttpDispatcher dispatcher;

  public RAPJakartaServlet(HttpDispatcher dispatcher, int timeoutSeconds, String basePath) {
    super(Duration.ofSeconds(timeoutSeconds), basePath);
    this.dispatcher = dispatcher;
  }

  public RAPJakartaServlet(TransportDispatcher dispatcher, int timeoutSeconds, String basePath) {
    this(HttpDispatcherFactory.create((dispatcher)), timeoutSeconds, basePath);
  }

  /**
   * Initialize the RAPJakartaServlet.
   *
   * @see #AbstractJakartaR2Servlet
   */
  public RAPJakartaServlet(
      HttpDispatcher dispatcher,
      boolean useContinuations,
      int timeOut,
      int timeOutDelta,
      String basePath) {
    super(Duration.ofSeconds(timeOut), basePath);
    this.dispatcher = dispatcher;
  }
}
