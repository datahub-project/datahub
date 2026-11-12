package com.linkedin.metadata.usage;

import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageDedupHeadersTest {

  @Test
  public void testIsPreRecordedWhenAbsent() {
    Assert.assertFalse(UsageDedupHeaders.isPreRecorded((Map<String, String>) null));
    Assert.assertFalse(UsageDedupHeaders.isPreRecorded(new MetadataChangeProposal()));
    Assert.assertFalse(UsageDedupHeaders.isPreRecorded(Map.of()));
  }

  @Test
  public void testIsPreRecordedWhenPresent() {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setHeaders(new StringMap(UsageDedupHeaders.withPreRecorded(null)));
    Assert.assertTrue(UsageDedupHeaders.isPreRecorded(mcp));
    Assert.assertTrue(UsageDedupHeaders.isPreRecorded(mcp.getHeaders()));
  }

  @Test
  public void testIsPreRecordedRejectsWrongValue() {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setHeaders(new StringMap(Map.of(UsageDedupHeaders.USAGE_PRE_RECORDED, "0")));
    Assert.assertFalse(UsageDedupHeaders.isPreRecorded(mcp));
  }

  @Test
  public void testStampPreRecordedPreservesExistingHeaders() {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal().setHeaders(new StringMap(Map.of("X-Custom", "a")));
    UsageDedupHeaders.stampPreRecorded(mcp);
    Assert.assertEquals(mcp.getHeaders().get("X-Custom"), "a");
    Assert.assertTrue(UsageDedupHeaders.isPreRecorded(mcp));
  }
}
