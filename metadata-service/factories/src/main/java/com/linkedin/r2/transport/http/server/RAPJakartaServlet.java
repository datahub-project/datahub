package com.linkedin.r2.transport.http.server;

import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import java.time.Duration;
import lombok.Getter;

public class RAPJakartaServlet extends AbstractJakartaR2Servlet {
  private static final long serialVersionUID = 0L;

  @Getter private final HttpDispatcher dispatcher;

  public RAPJakartaServlet(HttpDispatcher dispatcher, int timeoutSeconds) {
    super(Duration.ofSeconds(timeoutSeconds));
    this.dispatcher = dispatcher;
  }

  public RAPJakartaServlet(TransportDispatcher dispatcher, int timeoutSeconds) {
    this(HttpDispatcherFactory.create((dispatcher)), timeoutSeconds);
  }

  /**
   * Initialize the RAPJakartaServlet.
   *
   * @see #AbstractJakartaR2Servlet
   */
  public RAPJakartaServlet(
      HttpDispatcher dispatcher, boolean useContinuations, int timeOut, int timeOutDelta) {
    super(Duration.ofSeconds(timeOut));
    this.dispatcher = dispatcher;
  }
}
