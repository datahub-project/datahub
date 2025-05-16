package com.linkedin.metadata.timeseries.search;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.Operation;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import java.util.Map;
import org.testng.annotations.Test;

public class MappingsBuilderTest {
  @Test
  public void testMappingsOperations() {
    final AspectSpec aspectSpec =
        new EntitySpecBuilder().buildAspectSpec(new Operation().schema(), RecordTemplate.class);
    Map<String, Object> result = MappingsBuilder.getMappings(aspectSpec);
    assertEquals(result.size(), 1);
    Map<String, Object> properties = (Map<String, Object>) result.get("properties");
    assertEquals(properties.size(), 17);
    assertEquals(properties.get("actor"), ImmutableMap.of("type", "keyword"));
    assertEquals(properties.get("operationType"), ImmutableMap.of("type", "keyword"));
    assertEquals(properties.get("customOperationType"), ImmutableMap.of("type", "keyword"));
    assertEquals(properties.get("numAffectedRows"), ImmutableMap.of("type", "long"));
    assertEquals(properties.get("sourceType"), ImmutableMap.of("type", "keyword"));
    // this should have a date mapping since fieldType: DATETIME
    assertEquals(properties.get("lastUpdatedTimestamp"), ImmutableMap.of("type", "date"));
    // this should always have date type as the main timeseries field
    assertEquals(properties.get("timestampMillis"), ImmutableMap.of("type", "date"));
  }
}
