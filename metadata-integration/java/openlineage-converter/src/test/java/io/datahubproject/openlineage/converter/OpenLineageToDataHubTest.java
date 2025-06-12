package io.datahubproject.openlineage.converter;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.DatahubJob;
import java.io.IOException;
import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;

/** Tests for DatahubOpenlineageConfig and related functionality. */
public class OpenLineageToDataHubTest {

  @Test
  public void testEmptyJobDetection() throws IOException, URISyntaxException {
    // Create a job with no inputs and no outputs
    DatahubJob emptyJob = DatahubJob.builder().build();

    // Configure empty jobs exclusion
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().excludeEmptyJobs(true).build();

    // This would typically be done in the DatahubEventEmitter
    boolean isEmpty = emptyJob.getInSet().isEmpty() && emptyJob.getOutSet().isEmpty();
    assertTrue(isEmpty, "Job with no inputs and outputs should be detected as empty");
    assertTrue(config.isExcludeEmptyJobs(), "excludeEmptyJobs should be true");
  }
}
