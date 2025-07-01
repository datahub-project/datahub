package com.linkedin.metadata.kafka;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.DispatcherServlet;

@Component("dispatcherServlet")
public class CustomDispatcherServlet extends DispatcherServlet {

  @Override
  protected void doTrace(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
  }
}
