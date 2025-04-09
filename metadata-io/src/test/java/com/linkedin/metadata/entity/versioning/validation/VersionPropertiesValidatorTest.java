package com.linkedin.metadata.entity.versioning.validation;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
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
  private static final Urn MANAGED_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(managed,dataset)");
  private static final Urn LEXICOGRAPHIC_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(lexicographic,dataset)");
  private static final Urn ML_MODEL_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(managed,mlModel)");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");

  private SearchRetriever mockSearchRetriever;
  private MockAspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockSearchRetriever = Mockito.mock(SearchRetriever.class);
    when(mockSearchRetriever.scroll(
            anyList(), any(), nullable(String.class), anyInt(), anyList(), any()))
        .thenReturn(new ScrollResult().setEntities(new SearchEntityArray()));
    mockGraphRetriever = Mockito.mock(GraphRetriever.class);

    // Initialize mock aspect retriever with version set data
    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(
        MANAGED_VERSION_SET_URN,
        Arrays.asList(
            new VersionSetKey().setEntityType(DATASET_ENTITY_NAME),
            new VersionSetProperties()
                .setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB)));
    data.put(
        LEXICOGRAPHIC_VERSION_SET_URN,
        Arrays.asList(
            new VersionSetKey().setEntityType(DATASET_ENTITY_NAME),
            new VersionSetProperties().setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)));
    data.put(
        ML_MODEL_VERSION_SET_URN,
        Arrays.asList(
            new VersionSetKey().setEntityType(ML_MODEL_ENTITY_NAME),
            new VersionSetProperties().setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)));
    mockAspectRetriever = new MockAspectRetriever(data);
    mockAspectRetriever.setEntityRegistry(new TestEntityRegistry());

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
    properties.setVersionSet(MANAGED_VERSION_SET_URN);
    properties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);
    properties.setSortId("ABCDEFGH"); // Valid 8-char uppercase alpha
    properties.setVersion(new VersionTag().setVersionTag("123"));

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    Assert.assertTrue(validationResult.findAny().isEmpty());
  }

  @Test
  public void testInvalidSortId() {
    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(MANAGED_VERSION_SET_URN);
    properties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);
    properties.setSortId("123"); // Invalid - not 8 chars, not alpha
    properties.setVersion(new VersionTag().setVersionTag("123"));

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    AspectValidationException exception = validationResult.findAny().get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("Invalid sortID for Versioning Scheme"));
  }

  @Test
  public void testSortIdValidLexicographic() {
    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(LEXICOGRAPHIC_VERSION_SET_URN);
    properties.setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);
    properties.setSortId("123");
    properties.setVersion(new VersionTag().setVersionTag("123"));

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    var exceptions = validationResult.findAny();
    Assert.assertTrue(
        exceptions.isEmpty(), exceptions.map(AspectValidationException::getMessage).orElse(null));
  }

  @Test
  public void testVersioningSchemeMismatch() {
    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(MANAGED_VERSION_SET_URN);
    properties.setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);
    properties.setSortId("ABCDEFGH");
    properties.setVersion(new VersionTag().setVersionTag("123"));

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    AspectValidationException exception = validationResult.findAny().get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(
        exception.getMessage().contains("Versioning Scheme does not match Version Set properties"));
  }

  @Test
  public void testNonexistentVersionSet() {
    // Non-existent version set gets created by VersionPropertiesSideEffect
    Urn nonexistentUrn = UrnUtils.getUrn("urn:li:versionSet:(nonexistent,dataset)");

    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(nonexistentUrn);
    properties.setSortId("abc");
    properties.setVersion(new VersionTag().setVersionTag("123"));

    Stream<AspectValidationException> validationResult =
        VersionPropertiesValidator.validatePropertiesUpserts(
            TestMCP.ofOneUpsertItem(TEST_ENTITY_URN, properties, new TestEntityRegistry()),
            retrieverContext);

    var exceptions = validationResult.findAny();
    Assert.assertTrue(
        exceptions.isEmpty(), exceptions.map(AspectValidationException::getMessage).orElse(null));
  }

  @Test
  public void testEntityTypeMismatch() {
    VersionSetProperties versionSetProperties = new VersionSetProperties();
    versionSetProperties.setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);

    VersionProperties properties = new VersionProperties();
    properties.setVersionSet(ML_MODEL_VERSION_SET_URN);
    properties.setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);
    properties.setSortId("ABCDEFGH");
    properties.setVersion(new VersionTag().setVersionTag("123"));

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
    properties.setVersionSet(MANAGED_VERSION_SET_URN);
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
