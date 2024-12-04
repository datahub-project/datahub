package com.linkedin.metadata.entity.versioning.validation;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;

import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VersionPropertiesValidatorTest {

  private static final String ENTITY_TYPE = "dataset";
  private static final Urn TEST_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(12356,dataset)");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");

  private SearchRetriever mockSearchRetriever;
  private MockAspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockSearchRetriever = Mockito.mock(SearchRetriever.class);
    mockGraphRetriever = Mockito.mock(GraphRetriever.class);

    // Create version set key and properties
    VersionSetKey versionSetKey = new VersionSetKey();
    versionSetKey.setEntityType(ENTITY_TYPE);

    VersionSetProperties versionSetProperties = new VersionSetProperties();
    versionSetProperties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    // Initialize mock aspect retriever with version set data
    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(TEST_VERSION_SET_URN, Arrays.asList(versionSetKey, versionSetProperties));
    mockAspectRetriever = new MockAspectRetriever(data);

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .aspectRetriever(mockAspectRetriever)
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .build();
  }

  @Test
  public void testValidVersionProperties() {
    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(TEST_VERSION_SET_URN);
    properties.setSortId("ABCDEFGH"); // Valid 8-char uppercase alpha

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    Assert.assertTrue(validationResult.findAny().isEmpty());
  }

  @Test
  public void testInvalidSortId() {
    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(TEST_VERSION_SET_URN);
    properties.setSortId("123"); // Invalid - not 8 chars, not alpha

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    AspectValidationException exception = validationResult.findAny().get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("Invalid sortID for Versioning Scheme"));
  }

  @Test
  public void testNonexistentVersionSet() {
    Urn nonexistentUrn = UrnUtils.getUrn("urn:li:versionSet:(nonexistent,dataset)");

    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(nonexistentUrn);
    properties.setSortId("ABCDEFGH");

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    AspectValidationException exception = validationResult.findAny().get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("Version Set specified does not exist"));
  }

  @Test
  public void testEntityTypeMismatch() {
    // Create version set with different entity type
    VersionSetKey wrongTypeKey = new VersionSetKey();
    wrongTypeKey.setEntityType(CHART_ENTITY_NAME);

    VersionSetProperties versionSetProperties = new VersionSetProperties();
    versionSetProperties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(TEST_VERSION_SET_URN, Arrays.asList(wrongTypeKey, versionSetProperties));
    mockAspectRetriever = new MockAspectRetriever(data);

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .aspectRetriever(mockAspectRetriever)
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .build();

    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(TEST_VERSION_SET_URN);
    properties.setSortId("ABCDEFGH");

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    AspectValidationException exception = validationResult.findAny().get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(
        exception.getMessage().contains("Version Set specified entity type does not match"));
  }

  @Test
  public void testIsLatestFieldSpecified() {
    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(TEST_VERSION_SET_URN);
    properties.setSortId("ABCDEFGH");
    properties.setIsLatest(true); // Should not be specified

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesProposals(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()));

    AspectValidationException exception = validationResult.findAny().get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("IsLatest should not be specified"));
  }
}
