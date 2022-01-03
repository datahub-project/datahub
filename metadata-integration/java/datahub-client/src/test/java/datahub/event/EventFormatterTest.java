package datahub.event;

import com.linkedin.dataset.DatasetProperties;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.testng.Assert;


public class EventFormatterTest {

  @Test
  public void testPartialMCPW() throws URISyntaxException, IOException, EventValidationException {
    MetadataChangeProposalWrapper metadataChangeProposalWrapper = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn("urn:li:foo")
            .upsert()
        .aspect(new DatasetProperties().setDescription("A test dataset"))
        .build();
    EventFormatter eventFormatter = new EventFormatter();
    MetadataChangeProposal mcp = eventFormatter.convert(metadataChangeProposalWrapper);
    Assert.assertEquals(mcp.getAspect().getContentType(), "application/json");
    String content = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
    Assert.assertEquals(content, "{\"description\":\"A test dataset\"}");
  }
}
