package com.linkedin.restli.server;

import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.context.support.HttpRequestHandlerServlet;

@AllArgsConstructor
@NoArgsConstructor
@Component
public class RestliHandlerServlet extends HttpRequestHandlerServlet implements HttpRequestHandler {
  @Autowired private RAPJakartaServlet _r2Servlet;

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
