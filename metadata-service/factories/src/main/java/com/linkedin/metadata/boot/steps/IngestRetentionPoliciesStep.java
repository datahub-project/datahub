package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.retention.DataHubRetentionInfo;
import com.linkedin.retention.Retention;
import com.linkedin.retention.RetentionArray;
import com.linkedin.retention.VersionBasedRetention;
import java.io.IOException;
import java.net.URISyntaxException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;


@Slf4j
@RequiredArgsConstructor
public class IngestRetentionPoliciesStep implements BootstrapStep {

  private final RetentionService _retentionService;
  private final boolean _disableRetention;

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

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

    // 1. Read from the file into JSON.
    final JsonNode retentionPolicies = YAML_MAPPER.readTree(new ClassPathResource("./boot/retention.yaml").getFile());
    if (!retentionPolicies.isArray()) {
      throw new IllegalArgumentException("Retention config file must contain an array of retention policies");
    }
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

      
    }

    Retention defaultRetention =
        new Retention().setVersion(new VersionBasedRetention().setMaxVersions(_defaultMaxVersion));
    _retentionService.setRetention(null, null,
        new DataHubRetentionInfo().setRetentionPolicies(new RetentionArray(ImmutableList.of(defaultRetention))));
  }
}
