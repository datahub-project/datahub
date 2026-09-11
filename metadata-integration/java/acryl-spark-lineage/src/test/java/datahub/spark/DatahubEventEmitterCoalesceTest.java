package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.linkedin.common.FabricType;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Operation;
import com.linkedin.common.OperationType;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import java.net.URISyntaxException;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Spark coalesces OpenLineage events by default, so anything the coalescer fails to carry across is
 * converted and then silently dropped. That has now happened twice — once for dataset facets, once
 * for job-level ones — so the merge is pinned here rather than left to the Docker-bound smoke test.
 */
class DatahubEventEmitterCoalesceTest {

  private static DatahubJob decoratedJob() throws URISyntaxException {
    DatahubJob job = DatahubJob.builder().build();

    InstitutionalMemoryMetadata link = new InstitutionalMemoryMetadata();
    link.setUrl(new Url("https://github.com/acme/repo"));
    link.setDescription("Source code");
    job.setJobInstitutionalMemory(
        new InstitutionalMemory().setElements(new InstitutionalMemoryMetadataArray(link)));

    job.setJobGlobalTags(
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    new TagAssociation().setTag(TagUrn.createFromString("urn:li:tag:pii")))));

    job.setJobOwnership(
        new Ownership()
            .setOwners(
                new OwnerArray(
                    new Owner()
                        .setOwner(Urn.createFromString("urn:li:corpuser:alice"))
                        .setType(OwnershipType.DEVELOPER))));
    return job;
  }

  @Test
  void testJobDecorationSurvivesCoalescing() throws URISyntaxException {
    // The event that carried the facets is not the one the coalesced job was seeded from.
    DatahubJob coalesced = DatahubJob.builder().build();

    DatahubEventEmitter.mergeJobDecoration(coalesced, decoratedJob());

    assertNotNull(coalesced.getJobInstitutionalMemory(), "sourceCodeLocation link was dropped");
    assertEquals(
        "https://github.com/acme/repo",
        coalesced.getJobInstitutionalMemory().getElements().get(0).getUrl().toString());
    assertNotNull(coalesced.getJobGlobalTags(), "job tags were dropped");
    assertNotNull(coalesced.getJobOwnership(), "job owners were dropped");
  }

  @Test
  void testFirstEventWinsSoLaterEmptyEventsCannotClearDecoration() throws URISyntaxException {
    DatahubJob coalesced = DatahubJob.builder().build();
    DatahubEventEmitter.mergeJobDecoration(coalesced, decoratedJob());

    // A later event in the same run carries no facets at all; it must not undo the first.
    DatahubEventEmitter.mergeJobDecoration(coalesced, DatahubJob.builder().build());

    assertNotNull(coalesced.getJobInstitutionalMemory());
    assertNotNull(coalesced.getJobGlobalTags());
    assertNotNull(coalesced.getJobOwnership());
  }

  private static DatahubDataset datasetWithOperationAt(long timestamp) throws URISyntaxException {
    DatasetUrn urn =
        new DatasetUrn(new DataPlatformUrn("s3"), "my_db.my_schema.events", FabricType.PROD);
    Operation operation =
        new Operation()
            .setTimestampMillis(timestamp)
            .setOperationType(OperationType.INSERT)
            .setLastUpdatedTimestamp(timestamp);
    return DatahubDataset.builder().urn(urn).operation(operation).build();
  }

  /**
   * Operation and DatasetProfile are timeseries aspects, so each execution's point stands on its
   * own. Overwriting on merge meant a coalesced run that wrote the same dataset several times kept
   * only the last point and reported a single write.
   */
  @Test
  void testTimeseriesPointsAccumulateAcrossCoalescedEvents() throws URISyntaxException {
    Set<DatahubDataset> coalesced = new LinkedHashSet<>();
    coalesced.add(datasetWithOperationAt(1000L));

    DatahubEventEmitter.mergeDatasets(
        new LinkedHashSet<>(java.util.Collections.singletonList(datasetWithOperationAt(2000L))),
        coalesced);

    DatahubDataset merged = coalesced.iterator().next();
    assertEquals(2, merged.getOperations().size(), "the earlier execution's point was dropped");
    assertEquals(1000L, merged.getOperations().get(0).getTimestampMillis());
    assertEquals(2000L, merged.getOperations().get(1).getTimestampMillis());
  }
}
