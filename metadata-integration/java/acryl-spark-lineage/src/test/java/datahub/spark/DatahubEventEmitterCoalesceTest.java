package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import io.datahubproject.openlineage.dataset.DatahubJob;
import java.net.URISyntaxException;
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
}
