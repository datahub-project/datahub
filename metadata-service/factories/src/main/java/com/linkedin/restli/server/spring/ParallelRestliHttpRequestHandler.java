/*
   Copyright (c) 2013 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.linkedin.restli.server.spring;

import com.linkedin.metadata.filter.RestliLoggingFilter;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.r2.filter.FilterChain;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.transport.FilterChainDispatcher;
import com.linkedin.r2.transport.http.server.RAPServlet;
import com.linkedin.restli.server.DelegatingTransportDispatcher;
import com.linkedin.restli.server.RestLiConfig;
import com.linkedin.restli.server.RestLiServer;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.web.HttpRequestHandler;

public class ParallelRestliHttpRequestHandler implements HttpRequestHandler {

  private RAPServlet _r2Servlet;

  public ParallelRestliHttpRequestHandler(RestLiConfig config, SpringInjectResourceFactory injectResourceFactory) {
    this(config, injectResourceFactory, FilterChains.empty());
  }

  public ParallelRestliHttpRequestHandler(RestLiConfig config, SpringInjectResourceFactory injectResourceFactory,
      FilterChain filterChain) {
    config.addFilter(new RestliLoggingFilter());
    RestLiServer restLiServer = new RestLiServer(config, injectResourceFactory, getDefaultParseqEngine());
    _r2Servlet = new RAPServlet(
        new FilterChainDispatcher(new DelegatingTransportDispatcher(restLiServer, restLiServer), filterChain));
  }

  public Engine getDefaultParseqEngine() {
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    Engine engine = new EngineBuilder().setTaskExecutor(scheduler).setTimerScheduler(scheduler).build();
    return engine;
  }

  public ParallelRestliHttpRequestHandler(RAPServlet r2Servlet) {
    _r2Servlet = r2Servlet;
  }

  public void handleRequest(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    _r2Servlet.service(req, res);
  }
}
