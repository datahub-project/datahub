package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainUtilsTest {

  @Mock private QueryContext mockContext;
  @Mock private OperationContext mockOpContext;
  @Mock private EntityClient mockEntityClient;
  @Mock private EntityService<?> mockEntityService;
  @Mock private Authentication mockAuthentication;
  @Mock private AuthorizerChain mockAuthorizerChain;

  private Urn testEntityUrn;
  private Urn testDomainUrn;
  private Urn testActorUrn;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    testEntityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)");
    testDomainUrn = UrnUtils.getUrn("urn:li:domain:engineering");
    testActorUrn = UrnUtils.getUrn("urn:li:corpuser:test");
  }

  @Test
  public void testValidateDomain_Success() throws Exception {
    // Given
    org.mockito.Mockito.when(mockEntityService.exists(mockOpContext, testDomainUrn, true))
        .thenReturn(true);

    // When/Then - should not throw
    DomainUtils.validateDomain(mockOpContext, testDomainUrn, mockEntityService);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidateDomain_DomainDoesNotExist() throws Exception {
    // Given
    org.mockito.Mockito.when(mockEntityService.exists(mockOpContext, testDomainUrn, true))
        .thenReturn(false);

    // When/Then - should throw
    DomainUtils.validateDomain(mockOpContext, testDomainUrn, mockEntityService);
  }

  @Test
  public void testBuildParentDomainFilter_WithParent() throws Exception {
    // Given
    Urn parentDomainUrn = UrnUtils.getUrn("urn:li:domain:parent");

    // When
    Filter filter = DomainUtils.buildParentDomainFilter(parentDomainUrn);

    // Then
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 1);

    ConjunctiveCriterion criterion = filter.getOr().get(0);
    assertNotNull(criterion.getAnd());

    // Should have at least 2 criteria (hasParentDomain and parentDomain.keyword)
    assertTrue(
        criterion.getAnd().size() >= 2,
        "Should have at least hasParentDomain and parentDomain criteria");
  }

  @Test
  public void testBuildParentDomainFilter_RootDomain() {
    // When - null parent means root domain
    Filter filter = DomainUtils.buildParentDomainFilter(null);

    // Then
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertTrue(
        filter.getOr().size() > 0,
        "Root domain filter should have multiple OR conditions for hasParentDomain");

    // Should have OR conditions for hasParentDomain=false and null checks
    boolean hasFalseCriterion = false;
    boolean hasNullCriterion = false;

    for (ConjunctiveCriterion conjunctive : filter.getOr()) {
      for (Criterion c : conjunctive.getAnd()) {
        if (c.getField().equals("hasParentDomain")) {
          if (c.getCondition() == Condition.EQUAL && "false".equals(c.getValue())) {
            hasFalseCriterion = true;
          }
          if (c.getCondition() == Condition.IS_NULL) {
            hasNullCriterion = true;
          }
        }
      }
    }

    assertTrue(
        hasFalseCriterion || hasNullCriterion,
        "Root domain filter should check for false or null hasParentDomain");
  }

  @Test
  public void testBuildNameAndParentDomainFilter_WithNameAndParent() throws Exception {
    // Given
    String domainName = "TestDomain";
    Urn parentDomainUrn = UrnUtils.getUrn("urn:li:domain:parent");

    // When
    Filter filter = DomainUtils.buildNameAndParentDomainFilter(domainName, parentDomainUrn);

    // Then
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 1);

    ConjunctiveCriterion criterion = filter.getOr().get(0);
    assertNotNull(criterion.getAnd());
    // Should have multiple criteria including parent domain and name
    assertTrue(criterion.getAnd().size() >= 2, "Should have at least 2 criteria");
  }

  @Test
  public void testBuildNameAndParentDomainFilter_RootWithName() {
    // Given
    String domainName = "RootDomain";

    // When
    Filter filter = DomainUtils.buildNameAndParentDomainFilter(domainName, null);

    // Then
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    // Root domain filter should have multiple OR conditions for hasParentDomain variations
    assertTrue(filter.getOr().size() > 0, "Should have OR conditions for root domain");

    // Each OR condition should have criteria
    for (ConjunctiveCriterion conjunctive : filter.getOr()) {
      assertTrue(conjunctive.getAnd().size() > 0, "Each condition should have criteria");
    }
  }

  @Test
  public void testGetParentDomainSafely_WithParent() throws Exception {
    // Given
    Urn parentUrn = UrnUtils.getUrn("urn:li:domain:parent");
    DomainProperties properties = new DomainProperties();
    properties.setParentDomain(parentUrn);

    // When
    Urn result = DomainUtils.getParentDomainSafely(properties);

    // Then
    assertEquals(result, parentUrn);
  }

  @Test
  public void testGetParentDomainSafely_NoParent() {
    // Given
    DomainProperties properties = new DomainProperties();
    // Don't set parentDomain

    // When
    Urn result = DomainUtils.getParentDomainSafely(properties);

    // Then
    assertNull(result, "Should return null when no parent domain");
  }

  @Test
  public void testGetParentDomainSafely_ParentSetButHasParentFalse() throws Exception {
    // Given - test that parentDomain is returned when set
    Urn parentUrn = UrnUtils.getUrn("urn:li:domain:parent");
    DomainProperties properties = new DomainProperties();
    properties.setParentDomain(parentUrn);
    properties.setName("test");
    properties.setDescription("test");

    // When
    Urn result = DomainUtils.getParentDomainSafely(properties);

    // Then - the method returns parentDomain if hasParentDomain() returns true
    // When parentDomain is set, hasParentDomain() returns true
    assertNotNull(result, "Should return parent domain when field is set");
    assertEquals(result, parentUrn);
  }

  @Test
  public void testHasChildDomains_NoChildren() throws Exception {
    // Given
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    SearchResult emptyResult = new SearchResult();
    emptyResult.setNumEntities(0);
    emptyResult.setEntities(new com.linkedin.metadata.search.SearchEntityArray());

    org.mockito.Mockito.when(
            mockEntityClient.filter(
                eq(mockOpContext),
                eq(DOMAIN_ENTITY_NAME),
                any(Filter.class),
                isNull(),
                eq(0),
                eq(1)))
        .thenReturn(emptyResult);

    // When
    boolean result = DomainUtils.hasChildDomains(testDomainUrn, mockContext, mockEntityClient);

    // Then
    assertFalse(result, "Should return false when no child domains");
  }

  @Test
  public void testHasChildDomains_HasChildren() throws Exception {
    // Given
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    SearchEntity childEntity = new SearchEntity();
    childEntity.setEntity(UrnUtils.getUrn("urn:li:domain:child"));

    SearchResult resultWithChildren = new SearchResult();
    resultWithChildren.setNumEntities(1);
    com.linkedin.metadata.search.SearchEntityArray entities =
        new com.linkedin.metadata.search.SearchEntityArray();
    entities.add(childEntity);
    resultWithChildren.setEntities(entities);

    org.mockito.Mockito.when(
            mockEntityClient.filter(
                eq(mockOpContext),
                eq(DOMAIN_ENTITY_NAME),
                any(Filter.class),
                isNull(),
                eq(0),
                eq(1)))
        .thenReturn(resultWithChildren);

    // When
    boolean result = DomainUtils.hasChildDomains(testDomainUrn, mockContext, mockEntityClient);

    // Then
    assertTrue(result, "Should return true when child domains exist");
  }

  @Test
  public void testHasNameConflict_NoConflict() throws Exception {
    // Given
    String domainName = "UniqueDomain";
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    SearchResult emptyResult = new SearchResult();
    emptyResult.setEntities(new com.linkedin.metadata.search.SearchEntityArray());

    org.mockito.Mockito.when(
            mockEntityClient.filter(
                eq(mockOpContext),
                eq(DOMAIN_ENTITY_NAME),
                any(Filter.class),
                isNull(),
                eq(0),
                eq(1000)))
        .thenReturn(emptyResult);

    org.mockito.Mockito.when(
            mockEntityClient.batchGetV2(
                eq(mockOpContext), eq(DOMAIN_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(Collections.emptyMap());

    // When
    boolean result = DomainUtils.hasNameConflict(domainName, null, mockContext, mockEntityClient);

    // Then
    assertFalse(result, "Should return false when no name conflict");
  }

  @Test
  public void testHasNameConflict_HasConflict() throws Exception {
    // Given
    String domainName = "ConflictingDomain";
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    // Setup search result
    SearchEntity existingEntity = new SearchEntity();
    existingEntity.setEntity(testDomainUrn);

    SearchResult searchResult = new SearchResult();
    com.linkedin.metadata.search.SearchEntityArray entities =
        new com.linkedin.metadata.search.SearchEntityArray();
    entities.add(existingEntity);
    searchResult.setEntities(entities);

    org.mockito.Mockito.when(
            mockEntityClient.filter(
                eq(mockOpContext),
                eq(DOMAIN_ENTITY_NAME),
                any(Filter.class),
                isNull(),
                eq(0),
                eq(1000)))
        .thenReturn(searchResult);

    // Setup entity response with matching name
    DomainProperties properties = new DomainProperties();
    properties.setName(domainName);

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new com.linkedin.entity.Aspect(properties.data()));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap());
    entityResponse.getAspects().put(DOMAIN_PROPERTIES_ASPECT_NAME, envelopedAspect);

    Map<Urn, EntityResponse> responseMap = new HashMap<>();
    responseMap.put(testDomainUrn, entityResponse);

    org.mockito.Mockito.when(
            mockEntityClient.batchGetV2(
                eq(mockOpContext), eq(DOMAIN_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(responseMap);

    // When
    boolean result = DomainUtils.hasNameConflict(domainName, null, mockContext, mockEntityClient);

    // Then
    assertTrue(result, "Should return true when name conflict exists");
  }

  @Test
  public void testGetParentDomain_Success() throws Exception {
    // Given
    Urn parentDomainUrn = UrnUtils.getUrn("urn:li:domain:parent");
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    DomainProperties properties = new DomainProperties();
    properties.setParentDomain(parentDomainUrn);
    properties.setName("test");

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new com.linkedin.entity.Aspect(properties.data()));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap());
    entityResponse.getAspects().put(DOMAIN_PROPERTIES_ASPECT_NAME, envelopedAspect);

    org.mockito.Mockito.when(
            mockEntityClient.getV2(
                eq(mockOpContext), eq(DOMAIN_ENTITY_NAME), eq(testDomainUrn), anySet()))
        .thenReturn(entityResponse);

    // When
    com.linkedin.datahub.graphql.generated.Entity result =
        DomainUtils.getParentDomain(testDomainUrn, mockContext, mockEntityClient);

    // Then
    assertNotNull(result, "Should return parent domain entity");
  }

  @Test
  public void testGetParentDomain_NoParent() throws Exception {
    // Given
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);

    DomainProperties properties = new DomainProperties();
    properties.setName("test");
    // No parent domain set

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new com.linkedin.entity.Aspect(properties.data()));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap());
    entityResponse.getAspects().put(DOMAIN_PROPERTIES_ASPECT_NAME, envelopedAspect);

    org.mockito.Mockito.when(
            mockEntityClient.getV2(
                eq(mockOpContext), eq(DOMAIN_ENTITY_NAME), eq(testDomainUrn), anySet()))
        .thenReturn(entityResponse);

    // When
    com.linkedin.datahub.graphql.generated.Entity result =
        DomainUtils.getParentDomain(testDomainUrn, mockContext, mockEntityClient);

    // Then
    assertNull(result, "Should return null when no parent domain");
  }

  @Test
  public void testGetParentDomain_NoEntityResponse() throws Exception {
    // Given
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    org.mockito.Mockito.when(
            mockEntityClient.getV2(
                eq(mockOpContext), eq(DOMAIN_ENTITY_NAME), eq(testDomainUrn), anySet()))
        .thenReturn(null);

    // When
    com.linkedin.datahub.graphql.generated.Entity result =
        DomainUtils.getParentDomain(testDomainUrn, mockContext, mockEntityClient);

    // Then
    assertNull(result, "Should return null when entity response is null");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetParentDomain_Exception() throws Exception {
    // Given
    org.mockito.Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    org.mockito.Mockito.when(
            mockEntityClient.getV2(
                eq(mockOpContext), eq(DOMAIN_ENTITY_NAME), eq(testDomainUrn), anySet()))
        .thenThrow(new RuntimeException("Test exception"));

    // When/Then - should throw
    DomainUtils.getParentDomain(testDomainUrn, mockContext, mockEntityClient);
  }
}
