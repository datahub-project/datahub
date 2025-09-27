package com.linkedin.r2.transport.http.server;

import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import java.time.Duration;
import lombok.Getter;

public class RAPJakartaServlet extends AbstractJakartaR2Servlet {
  private static final long serialVersionUID = 0L;

  @Getter private final HttpDispatcher dispatcher;

  public RAPJakartaServlet(
      HttpDispatcher dispatcher, int timeoutSeconds, GMSConfiguration gmsConfiguration) {
    super(Duration.ofSeconds(timeoutSeconds), gmsConfiguration);
    this.dispatcher = dispatcher;
  }

  public RAPJakartaServlet(
      TransportDispatcher dispatcher, int timeoutSeconds, GMSConfiguration gmsConfiguration) {
    this(HttpDispatcherFactory.create((dispatcher)), timeoutSeconds, gmsConfiguration);
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
      GMSConfiguration gmsConfiguration) {
    super(Duration.ofSeconds(timeOut), gmsConfiguration);
    this.dispatcher = dispatcher;
  }
}
