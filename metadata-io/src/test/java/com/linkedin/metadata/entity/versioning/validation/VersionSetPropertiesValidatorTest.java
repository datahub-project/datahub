package com.linkedin.metadata.entity.versioning.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VersionSetPropertiesValidatorTest {

  private static final Urn TEST_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(123456,dataset)");

  private SearchRetriever mockSearchRetriever;
  private MockAspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockSearchRetriever = Mockito.mock(SearchRetriever.class);
    mockGraphRetriever = Mockito.mock(GraphRetriever.class);

    Map<Urn, List<RecordTemplate>> emptyData = new HashMap<>();
    mockAspectRetriever = new MockAspectRetriever(emptyData);

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .aspectRetriever(mockAspectRetriever)
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .build();
  }

  @Test
  public void testValidUpsertWithNoExistingProperties() {
    // Create version set properties
    VersionSetProperties properties =
        new VersionSetProperties()
            .setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    // Test validation with no existing properties
    Stream<AspectValidationException> validationResult =
        VersionSetPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_VERSION_SET_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    // Assert no validation exceptions
    Assert.assertTrue(validationResult.findAny().isEmpty());
  }

  @Test
  public void testValidUpsertWithSameVersioningScheme() {
    // Create existing properties with semantic versioning
    VersionSetProperties existingProperties =
        new VersionSetProperties()
            .setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    // Set up mock retriever with existing properties
    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(TEST_VERSION_SET_URN, Collections.singletonList(existingProperties));
    mockAspectRetriever = new MockAspectRetriever(data);

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .aspectRetriever(mockAspectRetriever)
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .build();

    // Create new properties with same versioning scheme
    VersionSetProperties newProperties =
        new VersionSetProperties()
            .setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    // Test validation
    Stream<AspectValidationException> validationResult =
        VersionSetPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_VERSION_SET_URN, newProperties, new TestEntityRegistry()),
            retrieverContext);

    // Assert no validation exceptions
    Assert.assertTrue(validationResult.findAny().isEmpty());
  }

  @Test
  public void testInvalidUpsertWithDifferentVersioningScheme() {
    // Create existing properties with semantic versioning
    VersionSetProperties existingProperties =
        new VersionSetProperties()
            .setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    // Set up mock retriever with existing properties
    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(TEST_VERSION_SET_URN, Collections.singletonList(existingProperties));
    mockAspectRetriever = new MockAspectRetriever(data);

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .aspectRetriever(mockAspectRetriever)
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .build();

    // Create new properties with different versioning scheme
    VersionSetProperties newProperties =
        new VersionSetProperties().setVersioningScheme(VersioningScheme.$UNKNOWN);

    // Test validation
    Stream<AspectValidationException> validationResult =
        VersionSetPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_VERSION_SET_URN, newProperties, new TestEntityRegistry()),
            retrieverContext);

    // Assert validation exception exists
    AspectValidationException exception = validationResult.findAny().get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("Versioning Scheme cannot change"));
    Assert.assertTrue(
        exception.getMessage().contains("Expected Scheme: ALPHANUMERIC_GENERATED_BY_DATAHUB"));
    Assert.assertTrue(exception.getMessage().contains("Provided Scheme: $UNKNOWN"));
  }
}
