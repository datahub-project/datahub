package com.linkedin.restli.server;

import com.linkedin.r2.transport.http.server.RAPServlet;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.context.support.HttpRequestHandlerServlet;

@Component
public class RestliHandlerServlet extends HttpRequestHandlerServlet implements HttpRequestHandler {
  @Autowired private RAPServlet _r2Servlet;

  @Override
  public void service(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    _r2Servlet.service(req, res);
  }

  @Override
  public void handleRequest(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    service(request, response);
  }
}
