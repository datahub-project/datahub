package com.linkedin.metadata.temporal.transformer;

import com.datahub.test.TestEntityComponentProfile;
import com.datahub.test.TestEntityComponentProfileArray;
import com.datahub.test.TestEntityProfile;
import com.datahub.test.TestEntitySnapshot;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongArray;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.temporal.TemporalInfo;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TemporalStatsTransformerTest {
  private final DataSchemaFactory dataSchemaFactory = new DataSchemaFactory("com.datahub.test");
  private final ConfigEntityRegistry entityRegistry =
      new ConfigEntityRegistry(dataSchemaFactory, TestEntitySnapshot.class.getClassLoader()
              .getResourceAsStream("test-entity-registry.yml"));
  private final AspectSpec profileAspectSpec =
      entityRegistry.getEntitySpec("testEntity").getAspectSpec("testEntityProfile");
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testTransformer() throws IOException {
    TemporalInfo temporalInfo = new TemporalInfo();
    temporalInfo.setEventTimestampMillis(0L);
    temporalInfo.setEventGranularityMillis(1000L);
    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    String urnStr = urn.toString();

    ObjectNode commonObject = JsonNodeFactory.instance.objectNode();
    commonObject.put("@timestamp", 0L);
    commonObject.put("eventTimestampMillis", 0L);
    commonObject.put("eventGranularityMillis", 1000L);
    commonObject.put("urn", urnStr);

    TestEntityProfile profile = new TestEntityProfile();
    profile.setTemporalInfo(temporalInfo);
    ObjectNode expectedOutput = commonObject;
    List<String> result = TemporalStatsTransformer.transform(urn, profile, profileAspectSpec);
    assertEquals(result.size(), 1);
    assertEquals(objectMapper.readTree(result.get(0)), objectMapper.readTree(expectedOutput.toString()));

    profile = new TestEntityProfile();
    profile.setTemporalInfo(temporalInfo);
    profile.setStat1(1L);
    expectedOutput = commonObject.deepCopy();
    expectedOutput.put("stat1", 1L);
    result = TemporalStatsTransformer.transform(urn, profile, profileAspectSpec);
    assertEquals(result.size(), 1);
    assertEquals(objectMapper.readTree(result.get(0)), objectMapper.readTree(expectedOutput.toString()));

    profile = new TestEntityProfile();
    profile.setTemporalInfo(temporalInfo);
    profile.setStat1(1L);
    profile.setStat2(1.0);
    profile.setStrStat("test");
    TestEntityComponentProfile componentProfile1 = new TestEntityComponentProfile();
    componentProfile1.setKey("key1");
    componentProfile1.setStat1(10L);
    componentProfile1.setStat2(10.0);
    componentProfile1.setStrStat("testComponent1");
    componentProfile1.setArrayStat(new LongArray(ImmutableList.of(1L, 2L)));
    TestEntityComponentProfile componentProfile2 = new TestEntityComponentProfile();
    componentProfile2.setKey("key2");
    componentProfile2.setStat1(20L);
    componentProfile2.setStat2(20.0);
    componentProfile2.setStrStat("testComponent2");
    componentProfile2.setArrayStat(new LongArray(ImmutableList.of()));
    profile.setComponentProfiles(
        new TestEntityComponentProfileArray(ImmutableList.of(componentProfile1, componentProfile2)));
    profile.setComponentProfiles2(new TestEntityComponentProfileArray(ImmutableList.of(componentProfile1)));
    ObjectNode expectedOutput1 = commonObject.deepCopy();
    expectedOutput1.put("stat1", 1L);
    expectedOutput1.put("overrideStat2", 1.0);
    expectedOutput1.put("strStat", "test");
    ObjectNode expectedOutputComponent1 = JsonNodeFactory.instance.objectNode();
    expectedOutputComponent1.put("key", "key1");
    expectedOutputComponent1.put("stat1", 10L);
    expectedOutputComponent1.put("overrideStat2", 10.0);
    expectedOutputComponent1.put("strStat", "testComponent1");
    expectedOutputComponent1.put("arrayStat", "[\"1\",\"2\"]");
    ObjectNode expectedOutputComponent2 = JsonNodeFactory.instance.objectNode();
    expectedOutputComponent2.put("key", "key2");
    expectedOutputComponent2.put("stat1", 20L);
    expectedOutputComponent2.put("overrideStat2", 20.0);
    expectedOutputComponent2.put("strStat", "testComponent2");
    expectedOutputComponent2.put("arrayStat", "[]");
    ObjectNode expectedOutput2 = commonObject.deepCopy();
    expectedOutput2.set("componentProfiles", expectedOutputComponent1);
    ObjectNode expectedOutput3 = commonObject.deepCopy();
    expectedOutput3.set("componentProfiles", expectedOutputComponent2);
    ObjectNode expectedOutput4 = commonObject.deepCopy();
    expectedOutput4.set("overrideComponentProfiles2", expectedOutputComponent1);
    List<ObjectNode> expectedOutputs =
        ImmutableList.of(expectedOutput1, expectedOutput2, expectedOutput3, expectedOutput4);
    result = TemporalStatsTransformer.transform(urn, profile, profileAspectSpec);
    assertEquals(result.size(), 4);
    Set<JsonNode> actual = new HashSet<>();
    for (String s : result) {
      actual.add(objectMapper.readTree(s));
    }
    Set<JsonNode> expected = new HashSet<>();
    for (ObjectNode output : expectedOutputs) {
      expected.add(objectMapper.readTree(output.toString()));
    }
    assertEquals(actual, expected);
  }
}
