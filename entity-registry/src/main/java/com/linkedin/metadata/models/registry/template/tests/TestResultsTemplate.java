package com.linkedin.metadata.models.registry.template.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.models.registry.template.CompoundKeyTemplate;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResults;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

public class TestResultsTemplate extends CompoundKeyTemplate<TestResults> {

  private static final String PASSING_FIELD_NAME = "passing";
  private static final String FAILING_FIELD_NAME = "failing";

  private static final List<String> KEY_FIELDS = Arrays.asList("test");

  @Override
  public Class<TestResults> getTemplateType() {
    return TestResults.class;
  }

  @Nonnull
  @Override
  public TestResults getDefault() {
    TestResults testResults = new TestResults();
    testResults.setFailing(new TestResultArray());
    testResults.setPassing(new TestResultArray());
    return testResults;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(
        arrayFieldToMap(baseNode, PASSING_FIELD_NAME, KEY_FIELDS), FAILING_FIELD_NAME, KEY_FIELDS);
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        transformedMapToArray(patched, PASSING_FIELD_NAME, KEY_FIELDS),
        FAILING_FIELD_NAME,
        KEY_FIELDS);
  }
}
