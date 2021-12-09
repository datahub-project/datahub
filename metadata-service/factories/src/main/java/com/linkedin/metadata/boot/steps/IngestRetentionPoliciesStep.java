package com.linkedin.metadata.boot.steps;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.retention.DataHubRetentionInfo;
import com.linkedin.retention.Retention;
import com.linkedin.retention.RetentionArray;
import com.linkedin.retention.VersionBasedRetention;
import java.io.IOException;
import java.net.URISyntaxException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class IngestRetentionPoliciesStep implements BootstrapStep {

  private final RetentionService _retentionService;
  private final boolean _disableRetention;
  private final int _defaultMaxVersion;

  @Override
  public String name() {
    return "IngestRetentionPoliciesStep";
  }

  @Override
  public void execute() throws IOException, URISyntaxException {
    // 0. Execute preflight check to see whether we need to ingest policies
    log.info("Ingesting default retention...");

    // Whether we are at clean boot or not.
    if (_disableRetention || _retentionService.hasRetention()) {
      log.info("Not eligible to run this step. Skipping.");
      return;
    }

    Retention defaultRetention =
        new Retention().setVersion(new VersionBasedRetention().setMaxVersions(_defaultMaxVersion));
    _retentionService.setRetention(null, null,
        new DataHubRetentionInfo().setRetentionPolicies(new RetentionArray(ImmutableList.of(defaultRetention))));
  }
}
