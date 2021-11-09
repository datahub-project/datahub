package com.datahub.gms.servlet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.version.GitVersion;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;


// Return a 200 for health checks
public class Config extends HttpServlet {
  ObjectMapper objectMapper = new ObjectMapper();

  private GitVersion getGitVersion(ServletContext servletContext) {
    WebApplicationContext ctx = WebApplicationContextUtils.getRequiredWebApplicationContext(servletContext);
    return (GitVersion) ctx.getBean("gitVersion");
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    Map<String, String> config = new HashMap<>();
    config.put("noCode", "true");

    GitVersion version = getGitVersion(req.getServletContext());
    config.put("version", version.getVersion());
    config.put("commit", version.getCommitId());

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
