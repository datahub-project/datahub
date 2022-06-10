package datahub.event;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;
import org.testng.Assert;

import com.linkedin.dataset.DatasetProperties;
import com.linkedin.mxe.MetadataChangeProposal;


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
  
  @Test
  public void testUtf8Encoding() throws URISyntaxException, IOException {

    MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)")
        .upsert()
        .aspect(new DatasetProperties().setDescription("This is the canonical User profile dataset œ∑´´†¥¨ˆˆπ“‘åß∂ƒ©˙∆˚¬…æΩ≈ç√∫˜˜≤≥ç"))
        .build();
    EventFormatter eventFormatter = new EventFormatter();
    MetadataChangeProposal mcp = eventFormatter.convert(mcpw);
    Assert.assertEquals(mcp.getAspect().getContentType(), "application/json");
    String content = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
    String expectedContent = "{\"description\":\"This is the canonical User profile dataset \\u0153\\u2211\\u00B4\\u00B4"
        + "\\u2020\\u00A5\\u00A8\\u02C6\\u02C6\\u03C0\\u201C\\u2018\\u00E5\\u00DF\\u2202\\u0192\\u00A9\\u02D9\\u2206"
        + "\\u02DA\\u00AC\\u2026\\u00E6\\u03A9\\u2248\\u00E7\\u221A\\u222B\\u02DC\\u02DC\\u2264\\u2265\\u00E7\"}";
    Assert.assertEquals(content, expectedContent);
  }
}
