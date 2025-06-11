package com.linkedin.datahub.graphql.types.test;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.TestMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestStatus;

public class TestMapperTest {

  @org.testng.annotations.Test
  public void testMapWithValidTestInfo() {
    // Arrange
    Urn testUrn = UrnUtils.getUrn("urn:li:test:test1");
    TestInfo testInfo = new TestInfo();
    testInfo.setCategory("Category1");
    testInfo.setName("Test1");
    testInfo.setDescription("Test Description");
    TestDefinition definition = new TestDefinition();
    definition.setJson("{\"key\": \"value\"}");
    testInfo.setDefinition(definition);
    TestStatus status = new TestStatus();
    status.setMode(com.linkedin.test.TestMode.ACTIVE);
    testInfo.setStatus(status);

    EnvelopedAspect envelopedTestInfo = new EnvelopedAspect();
    envelopedTestInfo.setValue(new com.linkedin.entity.Aspect(testInfo.data()));

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.TEST_INFO_ASPECT_NAME, envelopedTestInfo);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(testUrn);
    entityResponse.setAspects(aspects);

    // Act
    Test result = TestMapper.map(entityResponse);

    // Assert
    assertNotNull(result);
    assertEquals(result.getUrn(), testUrn.toString());
    assertEquals(result.getType(), EntityType.TEST);
    assertEquals(result.getCategory(), "Category1");
    assertEquals(result.getName(), "Test1");
    assertEquals(result.getDescription(), "Test Description");
    assertNotNull(result.getDefinition());
    assertEquals(result.getDefinition().getJson(), "{\"key\": \"value\"}");
    assertNotNull(result.getStatus());
    assertEquals(result.getStatus().getMode(), TestMode.ACTIVE);
  }

  @org.testng.annotations.Test
  public void testMapWithNullTestInfo() {
    // Arrange
    Urn testUrn = UrnUtils.getUrn("urn:li:test:test1");
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(testUrn);
    entityResponse.setAspects(aspects);

    // Act
    Test result = TestMapper.map(entityResponse);

    // Assert
    assertNull(result);
  }

  @org.testng.annotations.Test
  public void testMapWithNullStatus() {
    // Arrange
    Urn testUrn = UrnUtils.getUrn("urn:li:test:test1");
    TestInfo testInfo = new TestInfo();
    testInfo.setCategory("Category1");
    testInfo.setName("Test1");
    testInfo.setDescription("Test Description");
    TestDefinition definition = new TestDefinition();
    definition.setJson("{\"key\": \"value\"}");
    testInfo.setDefinition(definition);
    // Status is not set

    EnvelopedAspect envelopedTestInfo = new EnvelopedAspect();
    envelopedTestInfo.setValue(new com.linkedin.entity.Aspect(testInfo.data()));

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.TEST_INFO_ASPECT_NAME, envelopedTestInfo);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(testUrn);
    entityResponse.setAspects(aspects);

    // Act
    Test result = TestMapper.map(entityResponse);

    // Assert
    assertNotNull(result);
    assertEquals(result.getUrn(), testUrn.toString());
    assertEquals(result.getType(), EntityType.TEST);
    assertEquals(result.getCategory(), "Category1");
    assertEquals(result.getName(), "Test1");
    assertEquals(result.getDescription(), "Test Description");
    assertNotNull(result.getDefinition());
    assertEquals(result.getDefinition().getJson(), "{\"key\": \"value\"}");
    assertNull(result.getStatus());
  }
}
