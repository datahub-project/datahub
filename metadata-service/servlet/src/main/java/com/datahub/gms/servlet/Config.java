package com.datahub.gms.servlet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


// Return a 200 for health checks
public class Config extends HttpServlet {
  Map<String, String> config = new HashMap<String, String>() {{
    put("noCode", "true");
  }};
  ObjectMapper objectMapper = new ObjectMapper();


  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setContentType("application/json");
    PrintWriter out = resp.getWriter();

    try {
      String json = objectMapper.writeValueAsString(config);
      out.println(json);
      out.flush();
      resp.setStatus(200);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      resp.setStatus(500);
    }
  }
}
