package com.linkedin.metadata.structuredproperties.validators;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.structuredproperties.validation.ShowPropertyAsBadgeValidator;
import com.linkedin.structured.StructuredPropertySettings;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.Collections;
import java.util.stream.Stream;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ShowPropertyAsBadgeValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn TEST_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.privacy.retentionTime");
  private static final Urn EXISTING_BADGE_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.privacy.existingBadge");

  private SearchRetriever mockSearchRetriever;
  private MockAspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockSearchRetriever = Mockito.mock(SearchRetriever.class);

    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setShowAsAssetBadge(true)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);
    mockAspectRetriever =
        new MockAspectRetriever(
            ImmutableMap.of(
                TEST_PROPERTY_URN,
                Collections.singletonList(propertySettings),
                EXISTING_BADGE_URN,
                Collections.singletonList(propertySettings)));
    mockGraphRetriever = Mockito.mock(GraphRetriever.class);
    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .cachingAspectRetriever(mockAspectRetriever)
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .build();
  }

  @Test
  public void testValidUpsert() {

    // Create settings with showAsAssetBadge = true
    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setShowAsAssetBadge(true)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);

    Mockito.when(
            mockSearchRetriever.scroll(
                Mockito.eq(Collections.singletonList(STRUCTURED_PROPERTY_ENTITY_NAME)),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(10)))
        .thenReturn(new ScrollResult().setEntities(new SearchEntityArray()));

    // Test validation
    Stream<AspectValidationException> validationResult =
        ShowPropertyAsBadgeValidator.validateSettingsUpserts(
            TestMCP.ofOneUpsertItem(TEST_PROPERTY_URN, propertySettings, TEST_REGISTRY),
            retrieverContext);

    // Assert no validation exceptions
    Assert.assertTrue(validationResult.findAny().isEmpty());
  }

  @Test
  public void testInvalidUpsertWithExistingBadge() {

    // Create settings with showAsAssetBadge = true
    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setShowAsAssetBadge(true)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);

    // Mock search results with an existing badge
    SearchEntity existingBadge = new SearchEntity();
    existingBadge.setEntity(EXISTING_BADGE_URN);
    ScrollResult mockResult = new ScrollResult();
    mockResult.setEntities(new SearchEntityArray(Collections.singletonList(existingBadge)));
    Mockito.when(
            mockSearchRetriever.scroll(
                Mockito.eq(Collections.singletonList(STRUCTURED_PROPERTY_ENTITY_NAME)),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(10)))
        .thenReturn(mockResult);

    // Test validation
    Stream<AspectValidationException> validationResult =
        ShowPropertyAsBadgeValidator.validateSettingsUpserts(
            TestMCP.ofOneUpsertItem(TEST_PROPERTY_URN, propertySettings, TEST_REGISTRY),
            retrieverContext);

    // Assert validation exception exists
    Assert.assertFalse(validationResult.findAny().isEmpty());
  }

  @Test
  public void testValidUpsertWithShowAsAssetBadgeFalse() {

    // Create settings with showAsAssetBadge = false
    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setShowAsAssetBadge(false)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);

    // Mock search results with an existing badge (shouldn't matter since we're setting false)
    SearchEntity existingBadge = new SearchEntity();
    existingBadge.setEntity(EXISTING_BADGE_URN);
    ScrollResult mockResult = new ScrollResult();
    mockResult.setEntities(new SearchEntityArray(Collections.singletonList(existingBadge)));
    Mockito.when(
            mockSearchRetriever.scroll(
                Mockito.eq(Collections.singletonList(STRUCTURED_PROPERTY_ENTITY_NAME)),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(10)))
        .thenReturn(mockResult);

    // Test validation
    Stream<AspectValidationException> validationResult =
        ShowPropertyAsBadgeValidator.validateSettingsUpserts(
            TestMCP.ofOneUpsertItem(TEST_PROPERTY_URN, propertySettings, TEST_REGISTRY),
            retrieverContext);

    // Assert no validation exceptions
    Assert.assertTrue(validationResult.findAny().isEmpty());
  }
}
