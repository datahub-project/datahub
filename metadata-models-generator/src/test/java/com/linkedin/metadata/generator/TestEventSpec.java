package com.linkedin.metadata.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestEventSpec {

  static final String TEST_METADATA_MODELS_RESOLVED_PATH =
      "src/test/resources/com/linkedin/testing" + "../../metadata-testing/metadata-test-models/src/main/pegasus/:";
  static final String TEST_METADATA_MODELS_SOURCE_PATH = "src/test/resources/com/linkedin/testing";

  @Test
  public void testEventSpecParse() throws Exception {

    SchemaAnnotationRetriever schemaAnnotationRetriever =
        new SchemaAnnotationRetriever(TEST_METADATA_MODELS_RESOLVED_PATH);
    final String[] sources = {TEST_METADATA_MODELS_SOURCE_PATH};
    final List<EventSpec> eventSpecs = schemaAnnotationRetriever.generate(sources);

    // Check if annotations are correctly generated.
    final ArrayList<String> testList = new ArrayList<>();
    mapAspectToUrn(eventSpecs).get("com.linkedin.testing.FooUrn")
        .forEach(eventSpec -> testList.add(eventSpec.getValueType()));
    assertTrue(testList.containsAll(new ArrayList<>(Arrays.asList("AnnotatedAspectFoo", "AnnotatedAspectBar"))));
  }

  @Test
  public void testValidFullSchemaName() throws Exception {
    EventSpec tesEventSpec = new EventSpec();
    final String validFullSchemaName = "com.linkedin.testing.BarUrn";

    tesEventSpec.setValueType(validFullSchemaName);

    assertEquals(tesEventSpec.getValueType(), "BarUrn");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInValidFullSchemaName() throws Exception {
    EventSpec tesEventSpec = new EventSpec();
    final String validFullSchemaName = "com.linkedin.testing.BarUrn.";

    tesEventSpec.setValueType(validFullSchemaName);
  }

  @Nonnull
  private Map<String, ArrayList<EventSpec>> mapAspectToUrn(@Nonnull List<EventSpec> eventSpecs) {
    final Map<String, ArrayList<EventSpec>> eventsMap = new HashMap<>();
    for (EventSpec eventSpec : eventSpecs) {
      final Set<String> urnSet = eventSpec.getUrnSet();
      urnSet.forEach((urn) -> {
        if (eventsMap.containsKey(urn)) {
          eventsMap.get(urn).add(eventSpec);
        } else {
          eventsMap.put(urn, new ArrayList<>(Arrays.asList(eventSpec)));
        }
      });
    }
    return eventsMap;
  }
}