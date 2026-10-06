package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.Test;

public class AssertionRunSummaryPatchBuilderTest {

  private static class TestableBuilder extends AssertionRunSummaryPatchBuilder {
    List<ImmutableTriple<String, String, JsonNode>> pathValues() {
      return getPathValues();
    }

    String aspectName() {
      return getAspectName();
    }

    String entityType() {
      return getEntityType();
    }
  }

  @Test
  public void testBuildsEverySummaryField() throws Exception {
    TestableBuilder builder = new TestableBuilder();
    builder.urn(Urn.createFromString("urn:li:assertion:test"));
    builder
        .setLastPassedAt(100L)
        .setLastFailedAt(200L)
        .setLastErroredAt(300L)
        .setLastInitializedAt(400L)
        .setAssertionStatus("ERROR")
        .build();

    List<ImmutableTriple<String, String, JsonNode>> values = builder.pathValues();
    assertEquals(values.size(), 5);
    assertOperation(values.get(0), "/lastPassedAtMillis", 100L);
    assertOperation(values.get(1), "/lastFailedAtMillis", 200L);
    assertOperation(values.get(2), "/lastErroredAtMillis", 300L);
    assertOperation(values.get(3), "/lastInitializedAtMillis", 400L);
    assertEquals(values.get(4).getLeft(), "add");
    assertEquals(values.get(4).getMiddle(), "/assertionStatus");
    assertEquals(values.get(4).getRight().asText(), "ERROR");
    assertEquals(builder.aspectName(), "assertionRunSummary");
    assertEquals(builder.entityType(), "assertion");
  }

  private void assertOperation(
      ImmutableTriple<String, String, JsonNode> operation, String path, long value) {
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), path);
    assertEquals(operation.getRight().asLong(), value);
  }
}
