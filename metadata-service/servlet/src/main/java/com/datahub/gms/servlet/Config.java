package com.datahub.gms.servlet;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.config.EntityRegistryLoadResult;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

// Return a 200 for health checks

public class Config extends HttpServlet {
  Map<String, String> config = new HashMap<String, String>() {{
    put("noCode", "true");
  }};
  ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private Map<String, Map<ComparableVersion, EntityRegistryLoadResult>> getPluginModels(ServletContext servletContext) {
    WebApplicationContext ctx = WebApplicationContextUtils.getRequiredWebApplicationContext(servletContext);
    PluginEntityRegistryLoader pluginEntityRegistryLoader =
        (PluginEntityRegistryLoader) ctx.getBean("pluginEntityRegistry");
    Map<String, Map<ComparableVersion, Pair<EntityRegistry, EntityRegistryLoadResult>>> patchRegistries =
        pluginEntityRegistryLoader.getPatchRegistries();
    Map<String, Map<ComparableVersion, EntityRegistryLoadResult>> patchDiagnostics = new HashMap<>();
    patchRegistries.keySet().forEach(name -> patchDiagnostics.put(name, new HashMap<>()));

    patchRegistries.entrySet().forEach(entry -> {
      entry.getValue()
          .entrySet()
          .forEach(versionLoadEntry -> patchDiagnostics.get(entry.getKey())
              .put(versionLoadEntry.getKey(), versionLoadEntry.getValue().getSecond()));
    });
    return patchDiagnostics;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setContentType("application/json");
    PrintWriter out = resp.getWriter();

    try {
      Map<String, Object> config = new HashMap<>();
      config.put("noCode", "true");
      Map<String, Map<ComparableVersion, EntityRegistryLoadResult>> pluginTree =
          getPluginModels(req.getServletContext());
      config.put("models", pluginTree);
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
      out.println(json);
      out.flush();
      resp.setStatus(200);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      resp.setStatus(500);
    }
  }
}
