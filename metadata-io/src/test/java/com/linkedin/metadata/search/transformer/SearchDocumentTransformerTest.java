package com.linkedin.metadata.search.transformer;

import com.datahub.test.BrowsePaths;
import com.datahub.test.KeyPartEnum;
import com.datahub.test.SimpleNestedRecord1;
import com.datahub.test.SimpleNestedRecord2;
import com.datahub.test.SimpleNestedRecord2Array;
import com.datahub.test.TestEntityAspect;
import com.datahub.test.TestEntityAspectArray;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.datahub.test.TestEntitySnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.models.EntitySpec;
import java.io.IOException;
import java.util.Optional;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class SearchDocumentTransformerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testTransform() throws IOException {
    TestEntitySnapshot snapshot = buildSnapshot();
    EntitySpec testEntitySpec = TestEntitySpecBuilder.getSpec();
    Optional<String> result = SearchDocumentTransformer.transform(snapshot, testEntitySpec);
    assertTrue(result.isPresent());
    ObjectNode parsedJson = (ObjectNode) OBJECT_MAPPER.readTree(result.get());
    assertEquals(parsedJson.get("urn").asText(), snapshot.getUrn().toString());
    assertEquals(parsedJson.get("keyPart1").asText(), "key");
    assertFalse(parsedJson.has("keyPart2"));
    assertEquals(parsedJson.get("keyPart3").asText(), "VALUE_1");
    assertFalse(parsedJson.has("textField"));
    assertEquals(parsedJson.get("textFieldOverride").asText(), "test");
    ArrayNode textArrayField = (ArrayNode) parsedJson.get("textArrayField");
    assertEquals(textArrayField.size(), 2);
    assertEquals(textArrayField.get(0).asText(), "testArray1");
    assertEquals(textArrayField.get(1).asText(), "testArray2");
    assertEquals(parsedJson.get("nestedIntegerField").asInt(), 1);
    assertEquals(parsedJson.get("nestedForeignKey").asText(), snapshot.getUrn().toString());
    ArrayNode nextedArrayField = (ArrayNode) parsedJson.get("nestedArrayStringField");
    assertEquals(nextedArrayField.size(), 2);
    assertEquals(nextedArrayField.get(0).asText(), "nestedArray1");
    assertEquals(nextedArrayField.get(1).asText(), "nestedArray2");
    ArrayNode browsePaths = (ArrayNode) parsedJson.get("browsePaths");
    assertEquals(browsePaths.size(), 2);
    assertEquals(browsePaths.get(0).asText(), "/a/b/c");
    assertEquals(browsePaths.get(1).asText(), "d/e/f");
  }

  private TestEntitySnapshot buildSnapshot() {
    TestEntitySnapshot snapshot = new TestEntitySnapshot();
    TestEntityUrn urn = new TestEntityUrn("key", "urn", "VALUE_1");
    snapshot.setUrn(urn);

    TestEntityKey testEntityKey =
        new TestEntityKey().setKeyPart1("key").setKeyPart2(urn).setKeyPart3(KeyPartEnum.VALUE_1);

    TestEntityInfo testEntityInfo = new TestEntityInfo();
    testEntityInfo.setTextField("test");
    testEntityInfo.setTextArrayField(new StringArray(ImmutableList.of("testArray1", "testArray2")));
    testEntityInfo.setNestedRecordField(new SimpleNestedRecord1().setNestedIntegerField(1).setNestedForeignKey(urn));
    testEntityInfo.setNestedRecordArrayField(new SimpleNestedRecord2Array(
        ImmutableList.of(new SimpleNestedRecord2().setNestedArrayStringField("nestedArray1"),
            new SimpleNestedRecord2().setNestedArrayStringField("nestedArray2"))));

    BrowsePaths browsePaths = new BrowsePaths().setPaths(new StringArray(ImmutableList.of("/a/b/c", "d/e/f")));

    TestEntityAspectArray aspects = new TestEntityAspectArray(
        ImmutableList.of(TestEntityAspect.create(testEntityKey), TestEntityAspect.create(testEntityInfo),
            TestEntityAspect.create(browsePaths)));
    snapshot.setAspects(aspects);
    return snapshot;
  }
}
