package com.datahub.gms.servlet;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

// Return a 200 for health checks
public class HealthCheck extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
    resp.setStatus(200);
  }
}
