package com.linkedin.metadata.extractor;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.TestEntityUtil;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class FieldExtractorTest {
  @Test
  public void testExtractor() {
    EntitySpec testEntitySpec = TestEntitySpecBuilder.getSpec();
    AspectSpec testEntityInfoSpec = testEntitySpec.getAspectSpec("testEntityInfo");
    Map<String, SearchableFieldSpec> nameToSpec =
        testEntityInfoSpec.getSearchableFieldSpecs().stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getSearchableAnnotation().getFieldName(), Function.identity()));

    TestEntityInfo testEntityInfo = new TestEntityInfo();
    Map<SearchableFieldSpec, List<Object>> result =
        FieldExtractor.extractFields(testEntityInfo, testEntityInfoSpec.getSearchableFieldSpecs());
    assertEquals(
        result,
        testEntityInfoSpec.getSearchableFieldSpecs().stream()
            .collect(Collectors.toMap(Function.identity(), spec -> ImmutableList.of())));

    Urn urn = TestEntityUtil.getTestEntityUrn();
    testEntityInfo = TestEntityUtil.getTestEntityInfo(urn);
    result =
        FieldExtractor.extractFields(testEntityInfo, testEntityInfoSpec.getSearchableFieldSpecs());
    assertEquals(result.get(nameToSpec.get("textFieldOverride")), ImmutableList.of("test"));
    assertEquals(result.get(nameToSpec.get("foreignKey")), ImmutableList.of());
    assertEquals(result.get(nameToSpec.get("nestedForeignKey")), ImmutableList.of(urn));
    assertEquals(
        result.get(nameToSpec.get("textArrayField")), ImmutableList.of("testArray1", "testArray2"));
    assertEquals(result.get(nameToSpec.get("nestedIntegerField")), ImmutableList.of(1));
    assertEquals(
        result.get(nameToSpec.get("nestedArrayStringField")),
        ImmutableList.of("nestedArray1", "nestedArray2"));
    assertEquals(
        result.get(nameToSpec.get("nestedArrayArrayField")),
        ImmutableList.of("testNestedArray1", "testNestedArray2"));
    assertEquals(
        result.get(nameToSpec.get("customProperties")),
        ImmutableList.of("key1=value1", "key2=value2", "shortValue=123", "longValue=0123456789"));
    assertEquals(
        result.get(nameToSpec.get("esObjectField")),
        ImmutableList.of("key1=value1", "key2=value2", "shortValue=123", "longValue=0123456789"));
  }

  @Test
  public void testExtractorMaxValueLength() {
    EntitySpec testEntitySpec = TestEntitySpecBuilder.getSpec();
    AspectSpec testEntityInfoSpec = testEntitySpec.getAspectSpec("testEntityInfo");
    Map<String, SearchableFieldSpec> nameToSpec =
        testEntityInfoSpec.getSearchableFieldSpecs().stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getSearchableAnnotation().getFieldName(), Function.identity()));

    TestEntityInfo testEntityInfo = new TestEntityInfo();
    Map<SearchableFieldSpec, List<Object>> result =
        FieldExtractor.extractFields(testEntityInfo, testEntityInfoSpec.getSearchableFieldSpecs());
    assertEquals(
        result,
        testEntityInfoSpec.getSearchableFieldSpecs().stream()
            .collect(Collectors.toMap(Function.identity(), spec -> ImmutableList.of())));

    Urn urn = TestEntityUtil.getTestEntityUrn();
    testEntityInfo = TestEntityUtil.getTestEntityInfo(urn);
    result =
        FieldExtractor.extractFields(
            testEntityInfo, testEntityInfoSpec.getSearchableFieldSpecs(), 1);
    assertEquals(result.get(nameToSpec.get("textFieldOverride")), ImmutableList.of("test"));
    assertEquals(result.get(nameToSpec.get("foreignKey")), ImmutableList.of());
    assertEquals(result.get(nameToSpec.get("nestedForeignKey")), ImmutableList.of(urn));
    assertEquals(
        result.get(nameToSpec.get("textArrayField")), ImmutableList.of("testArray1", "testArray2"));
    assertEquals(result.get(nameToSpec.get("nestedIntegerField")), ImmutableList.of(1));
    assertEquals(
        result.get(nameToSpec.get("nestedArrayStringField")),
        ImmutableList.of("nestedArray1", "nestedArray2"));
    assertEquals(
        result.get(nameToSpec.get("nestedArrayArrayField")),
        ImmutableList.of("testNestedArray1", "testNestedArray2"));
    assertEquals(
        result.get(nameToSpec.get("customProperties")),
        ImmutableList.of(),
        "Expected no matching values because of value limit of 1");
    assertEquals(
        result.get(nameToSpec.get("esObjectField")),
        ImmutableList.of(),
        "Expected no matching values because of value limit of 1");
  }
}
