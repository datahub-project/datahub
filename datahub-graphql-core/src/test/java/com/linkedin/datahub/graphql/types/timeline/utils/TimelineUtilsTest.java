package com.linkedin.datahub.graphql.types.timeline.utils;

import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.graphql.generated.SchemaFieldChange;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import org.testng.annotations.Test;

public class TimelineUtilsTest {

  private static ChangeEvent addEvent() {
    return ChangeEvent.builder()
        .operation(ChangeOperation.ADD)
        .category(ChangeCategory.TECHNICAL_SCHEMA)
        .description("Added field")
        .build();
  }

  @Test
  public void testGetLastSchemaFieldChangeKeepsVersionStamp() {
    SchemaFieldChange change =
        TimelineUtils.getLastSchemaFieldChange(addEvent(), 1L, "1.0.0", "schemaMetadata:2");
    assertEquals(change.getVersionStamp(), "schemaMetadata:2");
  }

  @Test
  public void testGetLastSchemaFieldChangeDefaultsMissingVersionStamp() {
    // SchemaFieldChange.versionStamp is String! in the schema; a null here fails the whole blame.
    SchemaFieldChange change =
        TimelineUtils.getLastSchemaFieldChange(addEvent(), 1L, "1.0.0", null);
    assertEquals(change.getVersionStamp(), "none");
  }
}
