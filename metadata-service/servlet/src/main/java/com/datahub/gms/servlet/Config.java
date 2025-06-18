package com.datahub.gms.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.config.EntityRegistryLoadResult;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

// Return a 200 for health checks

@Slf4j
public class Config extends HttpServlet {
  private static final long CACHE_DURATION_SECONDS = 60;

  private static final Map<String, Object> BASE_CONFIG =
      Map.of(
          "noCode",
          "true",
          "retention",
          "true",
          "statefulIngestionCapable",
          true,
          "patchCapable",
          true,
          "timeZone",
          ZoneId.systemDefault().toString());

  private volatile Map<String, Object> cachedConfig;
  private volatile Instant lastUpdated;
  private final ReadWriteLock configLock = new ReentrantReadWriteLock();
  private ObjectMapper objectMapper;

  private void updateConfigCache(@Nonnull ServletContext servletContext) {
    configLock.writeLock().lock();
    try {
      WebApplicationContext ctx =
          WebApplicationContextUtils.getRequiredWebApplicationContext(servletContext);

      // Retrieve SystemOperationContext and ObjectMapper (if not already retrieved)
      if (objectMapper == null) {
        OperationContext systemOperationContext =
            ctx.getBean("systemOperationContext", OperationContext.class);
        objectMapper = systemOperationContext.getObjectMapper();
      }

      // Create a thread-safe, modifiable configuration
      Map<String, Object> newConfig = new ConcurrentHashMap<>(BASE_CONFIG);

      // Add static configurations
      ConfigurationProvider configProvider = getConfigProvider(ctx);
      newConfig.put("supportsImpactAnalysis", checkImpactAnalysisSupport(ctx));

      // Version Configuration
      GitVersion version = getGitVersion(ctx);
      Map<String, Object> versionConfig = new HashMap<>();
      versionConfig.put("acryldata/datahub", version.toConfig());
      newConfig.put("versions", versionConfig);

      // Telemetry Configuration
      Map<String, Object> telemetryConfig = new HashMap<>();
      telemetryConfig.put("enabledCli", configProvider.getTelemetry().enabledCli);
      telemetryConfig.put("enabledIngestion", configProvider.getTelemetry().enabledIngestion);

      newConfig.put("telemetry", telemetryConfig);

      // Ingestion Configuration
      Map<String, Object> ingestionConfig = new HashMap<>();
      ingestionConfig.put("enabled", configProvider.getIngestion().enabled);
      ingestionConfig.put("defaultCliVersion", configProvider.getIngestion().defaultCliVersion);
      newConfig.put("managedIngestion", ingestionConfig);

      // DataHub Configuration
      Map<String, Object> datahubConfig = new HashMap<>();
      datahubConfig.put("serverType", configProvider.getDatahub().serverType);
      datahubConfig.put("serverEnv", configProvider.getDatahub().serverEnv);
      newConfig.put("datahub", datahubConfig);

      // Dataset URN Name Casing
      Boolean datasetUrnNameCasing = getDatasetUrnNameCasing(ctx);
      newConfig.put("datasetUrnNameCasing", datasetUrnNameCasing);

      // Plugin Models (most dynamic part)
      Map<String, Map<ComparableVersion, EntityRegistryLoadResult>> pluginTree =
          getPluginModels(servletContext);
      newConfig.put("models", pluginTree);

      // Update cache and timestamp
      cachedConfig = Collections.unmodifiableMap(newConfig);
      lastUpdated = Instant.now();
    } catch (Exception e) {
      log.error("Failed to update configuration cache", e);
      throw new RuntimeException("Configuration update failed", e);
    } finally {
      configLock.writeLock().unlock();
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    // Check if cache needs refresh
    if (cachedConfig == null
        || Instant.now().minusSeconds(CACHE_DURATION_SECONDS).isAfter(lastUpdated)) {

      configLock.writeLock().lock();
      try {
        // Recheck condition after acquiring write lock
        if (cachedConfig == null
            || Instant.now().minusSeconds(CACHE_DURATION_SECONDS).isAfter(lastUpdated)) {
          updateConfigCache(req.getServletContext());
        }
      } finally {
        configLock.writeLock().unlock();
      }
    }

    // Serialize cached configuration
    configLock.readLock().lock();
    try {
      resp.setContentType("application/json");
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(cachedConfig);
      resp.getWriter().println(json);
      resp.setStatus(200);
    } catch (Exception e) {
      log.error("Serialization error", e);
      resp.setStatus(500);
    } finally {
      configLock.readLock().unlock();
    }
  }

  private static Map<String, Map<ComparableVersion, EntityRegistryLoadResult>> getPluginModels(
      ServletContext servletContext) {
    WebApplicationContext ctx =
        WebApplicationContextUtils.getRequiredWebApplicationContext(servletContext);
    PluginEntityRegistryLoader pluginEntityRegistryLoader =
        (PluginEntityRegistryLoader) ctx.getBean("pluginEntityRegistry");
    Map<String, Map<ComparableVersion, Pair<EntityRegistry, EntityRegistryLoadResult>>>
        patchRegistries = pluginEntityRegistryLoader.getPatchRegistries();
    Map<String, Map<ComparableVersion, EntityRegistryLoadResult>> patchDiagnostics =
        new HashMap<>();
    patchRegistries.keySet().forEach(name -> patchDiagnostics.put(name, new HashMap<>()));

    patchRegistries
        .entrySet()
        .forEach(
            entry -> {
              entry
                  .getValue()
                  .entrySet()
                  .forEach(
                      versionLoadEntry ->
                          patchDiagnostics
                              .get(entry.getKey())
                              .put(
                                  versionLoadEntry.getKey(),
                                  versionLoadEntry.getValue().getSecond()));
            });
    return patchDiagnostics;
  }

  private static ConfigurationProvider getConfigProvider(WebApplicationContext ctx) {
    return (ConfigurationProvider) ctx.getBean("configurationProvider");
  }

  private static GitVersion getGitVersion(WebApplicationContext ctx) {
    return (GitVersion) ctx.getBean("gitVersion");
  }

  private static Boolean getDatasetUrnNameCasing(WebApplicationContext ctx) {
    return (Boolean) ctx.getBean("datasetUrnNameCasing");
  }

  private static boolean checkImpactAnalysisSupport(WebApplicationContext ctx) {
    return ((GraphService) ctx.getBean("graphService")).supportsMultiHop();
  }
}
