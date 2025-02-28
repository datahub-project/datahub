package com.linkedin.restli.server;

import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.context.support.HttpRequestHandlerServlet;

@Slf4j
@AllArgsConstructor
public class RestliHandlerServlet extends HttpRequestHandlerServlet implements HttpRequestHandler {
  private final RAPJakartaServlet r2Servlet;

  @Override
  public void init(ServletConfig config) throws ServletException {
    log.info("Initializing RestliHandlerServlet");
    this.r2Servlet.init(config);
    log.info("Initialized RestliHandlerServlet");
  }

  @Override
  public void service(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    r2Servlet.service(req, res);
  }

  @Override
  public void handleRequest(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    service(request, response);
  }
}
