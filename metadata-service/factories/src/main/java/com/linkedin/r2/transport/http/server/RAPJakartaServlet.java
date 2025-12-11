/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
