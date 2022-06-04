package com.datahub.gms.servlet;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.config.EntityRegistryLoadResult;
import com.linkedin.metadata.version.GitVersion;
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

  Map<String, Object> config = new HashMap<String, Object>() {{
    put("noCode", "true");
    put("retention", "true");
    put("statefulIngestionCapable", true);
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

  private ConfigurationProvider getConfigProvider(WebApplicationContext ctx) {
    return (ConfigurationProvider) ctx.getBean("configurationProvider");
  }

  private GitVersion getGitVersion(WebApplicationContext ctx) {
    return (GitVersion) ctx.getBean("gitVersion");
  }

  private Boolean getDatasetUrnNameCasing(WebApplicationContext ctx) {
    return (Boolean) ctx.getBean("datasetUrnNameCasing");
  }

  private boolean checkImpactAnalysisSupport(WebApplicationContext ctx) {
    return ((GraphService) ctx.getBean("graphService")).supportsMultiHop();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    config.put("noCode", "true");

    WebApplicationContext ctx = WebApplicationContextUtils.getRequiredWebApplicationContext(req.getServletContext());

    config.put("supportsImpactAnalysis", checkImpactAnalysisSupport(ctx));

    GitVersion version = getGitVersion(ctx);
    Map<String, Object> versionConfig = new HashMap<>();
    versionConfig.put("linkedin/datahub", version.toConfig());
    config.put("versions", versionConfig);

    ConfigurationProvider configProvider = getConfigProvider(ctx);

    Map<String, Object> telemetryConfig = new HashMap<String, Object>() {{
      put("enabledCli", configProvider.getTelemetry().enabledCli);
      put("enabledIngestion", configProvider.getTelemetry().enabledIngestion);
    }};
    config.put("telemetry", telemetryConfig);

    Map<String, Object> ingestionConfig = new HashMap<String, Object>() {{
      put("enabled", configProvider.getIngestion().enabled);
      put("defaultCliVersion", configProvider.getIngestion().defaultCliVersion);
    }};
    config.put("managedIngestion", ingestionConfig);

    Map<String, Object> datahubConfig = new HashMap<String, Object>() {{
      put("serverType", configProvider.getDatahub().serverType);
    }};
    config.put("datahub", datahubConfig);

    resp.setContentType("application/json");
    PrintWriter out = resp.getWriter();

    Boolean datasetUrnNameCasing = getDatasetUrnNameCasing(ctx);
    config.put("datasetUrnNameCasing", datasetUrnNameCasing);

    try {
      Map<String, Object> config = new HashMap<>(this.config);
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
