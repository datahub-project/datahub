package com.linkedin.metadata.aspect.patch.template;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.aspect.patch.template.usagefeatures.UsageFeaturesTemplate;
import com.linkedin.metadata.search.features.UsageFeatures;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import org.testng.annotations.Test;

public class UsageFeaturesTemplateTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testPatchUsageFeatures() throws Exception {
    UsageFeaturesTemplate usageFeaturesTemplate = new UsageFeaturesTemplate();
    UsageFeatures usageFeatures = usageFeaturesTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.add("/writeCountRankLast30Days", Json.createValue(7L));

    // Initial population test
    UsageFeatures result =
        usageFeaturesTemplate.applyPatch(usageFeatures, jsonPatchBuilder.build());
    assertEquals(result.getWriteCountRankLast30Days(), 7L);

    // Test non-overwrite usage metrics and correct usage score
    JsonObjectBuilder usageNode2 = Json.createObjectBuilder();
    usageNode2.add("writeCountLast30Days", Json.createValue(25L));

    JsonPatchBuilder patchOperations2 = Json.createPatchBuilder();
    patchOperations2.add("/writeCountLast30Days", Json.createValue(25L));

    JsonPatch jsonPatch2 = patchOperations2.build();
    UsageFeatures result2 = usageFeaturesTemplate.applyPatch(result, jsonPatch2);
    assertEquals(result2.getWriteCountRankLast30Days(), 7L);
    assertEquals(result2.getWriteCountLast30Days(), 25L);

    // Remove
    JsonPatchBuilder removeOperations = Json.createPatchBuilder();
    removeOperations.remove("/writeCountLast30Days");

    JsonPatch removePatch = removeOperations.build();
    UsageFeatures finalResult = usageFeaturesTemplate.applyPatch(result2, removePatch);
    assertNull(finalResult.getWriteCountLast30Days());
  }
}
