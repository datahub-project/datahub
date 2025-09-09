package com.linkedin.metadata.service.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.List;
import org.testng.Assert;

public class MCPUtils {
  public record TestMCP(Urn entityUrn, RecordTemplate aspect) {}

  public static void assertEquals(List<MetadataChangeProposal> actual, List<TestMCP> expected) {
    Assert.assertEquals(actual.size(), expected.size());
    for (int i = 0; i < actual.size(); i++) {
      assertEquals(actual.get(i), expected.get(i), i);
    }
  }

  public static void assertEquals(MetadataChangeProposal actual, TestMCP expected, int i) {
    Assert.assertEquals(
        actual.getEntityUrn(),
        expected.entityUrn,
        String.format("Entity urns not equal for index %d", i));

    // Not comparing deserialized aspects immediately because for some reason,
    // AuditStamp.time is getting converted to int by deserializeAspect, breaking equality.
    if (!actual.getAspect().equals(GenericRecordUtils.serializeAspect(expected.aspect))) {
      RecordTemplate actualAspect =
          GenericRecordUtils.deserializeAspect(
              actual.getAspect().getValue(),
              actual.getAspect().getContentType(),
              expected.aspect.getClass());

      // Easier to debug than comparison of byte strings
      Assert.assertEquals(
          actualAspect,
          expected.aspect,
          String.format("Aspects not equal for urn %s", actual.getEntityUrn()));
    }
  }
}
