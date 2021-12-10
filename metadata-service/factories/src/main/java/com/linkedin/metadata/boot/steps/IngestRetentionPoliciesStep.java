package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.retention.DataHubRetentionInfo;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;


@Slf4j
@RequiredArgsConstructor
public class IngestRetentionPoliciesStep implements BootstrapStep {

  private final RetentionService _retentionService;
  private final boolean _disableRetention;
  private final String pluginPath;

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public String name() {
    return "IngestRetentionPoliciesStep";
  }

  @Override
  public void execute() throws IOException, URISyntaxException {
    // 0. Execute preflight check to see whether we need to ingest policies
    log.info("Ingesting default retention...");

    // Whether we are at clean boot or not.
    if (_disableRetention) {
      log.info("IngestRetentionPolicies disabled. Skipping.");
      return;
    }

    // 1. Read default retention config
    final Map<DataHubRetentionKey, DataHubRetentionInfo> retentionPolicyMap =
        parseFileOrDir(new ClassPathResource("./boot/retention.yaml").getFile());

    // 2. Read plugin retention config files from input path and overlay
    retentionPolicyMap.putAll(parseFileOrDir(new File(pluginPath)));

    // 4. Set the specified retention policies
    boolean hasUpdate = false;
    for (DataHubRetentionKey key : retentionPolicyMap.keySet()) {
      if (_retentionService.setRetention(key.getEntityName(), key.getAspectName(), retentionPolicyMap.get(key))) {
        hasUpdate = true;
      }
    }

    // 5. If there were updates on any of the retention policies, apply retention to all records
    if (hasUpdate) {
      _retentionService.batchApplyRetention(null, null);
    }
  }

  // Parse input yaml file or yaml files in the input directory to generate a retention policy map
  private Map<DataHubRetentionKey, DataHubRetentionInfo> parseFileOrDir(File retentionFileOrDir) throws IOException {
    // If path does not exist return empty
    if (!retentionFileOrDir.exists()) {
      return Collections.emptyMap();
    }

    // If directory, parse the yaml files under the directory
    if (retentionFileOrDir.isDirectory()) {
      Map<DataHubRetentionKey, DataHubRetentionInfo> result = new HashMap<>();

      for (File retentionFile : retentionFileOrDir.listFiles()) {
        if (!retentionFile.isFile()) {
          log.info("Element {} in plugin directory {} is not a file. Skipping", retentionFile.getPath(),
              retentionFileOrDir.getPath());
          continue;
        }
        result.putAll(parseFileOrDir(retentionFile));
      }
      return result;
    }
    // If file, parse the yaml file and return result;
    if (!retentionFileOrDir.getPath().endsWith(".yaml") && retentionFileOrDir.getPath().endsWith(".yml")) {
      log.info("File {} is not a YAML file. Skipping", retentionFileOrDir.getPath());
      return Collections.emptyMap();
    }
    return parseYamlRetentionConfig(retentionFileOrDir);
  }

  private Map<DataHubRetentionKey, DataHubRetentionInfo> parseYamlRetentionConfig(File retentionConfigFile)
      throws IOException {
    final JsonNode retentionPolicies = YAML_MAPPER.readTree(retentionConfigFile);
    if (!retentionPolicies.isArray()) {
      throw new IllegalArgumentException("Retention config file must contain an array of retention policies");
    }

    Map<DataHubRetentionKey, DataHubRetentionInfo> retentionPolicyMap = new HashMap<>();

    for (JsonNode retentionPolicy : retentionPolicies) {
      DataHubRetentionKey key = new DataHubRetentionKey();
      if (retentionPolicy.has("entity")) {
        key.setEntityName(retentionPolicy.get("entity").asText());
      } else {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field entity. Set to * for setting defaults");
      }

      if (retentionPolicy.has("aspect")) {
        key.setAspectName(retentionPolicy.get("aspect").asText());
      } else {
        throw new IllegalArgumentException(
            "Each element in the retention config must contain field aspect. Set to * for setting defaults");
      }

      DataHubRetentionInfo retentionInfo;
      if (retentionPolicy.has("retention")) {
        retentionInfo =
            RecordUtils.toRecordTemplate(DataHubRetentionInfo.class, retentionPolicy.get("retention").toString());
      } else {
        throw new IllegalArgumentException("Each element in the retention config must contain field retention");
      }

      retentionPolicyMap.put(key, retentionInfo);
    }
    return retentionPolicyMap;
  }
}
