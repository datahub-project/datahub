package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainUtilsTest {

  private static final String DOMAIN_URN = "urn:li:domain:test-domain";
  private static final String PARENT_DOMAIN_URN = "urn:li:domain:parent-domain";
  private static final String GLOSSARY_TERM_URN =
      "urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451";
  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test,TEST)";

  private EntityClient mockEntityClient;

  @BeforeMethod
  public void setUp() {
    mockEntityClient = Mockito.mock(EntityClient.class);
  }

  @Test
  public void testIsAuthorizedToUpdateDomainsForEntityWithPrivilege() {
    QueryContext allowContext = getMockAllowContext();
    Urn entityUrn = UrnUtils.getUrn(DATASET_URN);
    assertTrue(
        DomainUtils.isAuthorizedToUpdateDomainsForEntity(
            allowContext, entityUrn, mockEntityClient));
  }

  @Test
  public void testIsAuthorizedToUpdateDomainsForEntityDenied() {
    QueryContext denyContext = getMockDenyContext();
    Urn entityUrn = UrnUtils.getUrn(DATASET_URN);
    assertFalse(
        DomainUtils.isAuthorizedToUpdateDomainsForEntity(denyContext, entityUrn, mockEntityClient));
  }

  @Test
  public void testIsAuthorizedToUpdateDomainsForGlossaryEntity() {
    QueryContext allowContext = getMockAllowContext();
    Urn glossaryTermUrn = UrnUtils.getUrn(GLOSSARY_TERM_URN);
    assertTrue(
        DomainUtils.isAuthorizedToUpdateDomainsForEntity(
            allowContext, glossaryTermUrn, mockEntityClient));
  }

  @Test
  public void testBuildNameAndParentDomainFilterWithNameAndNullParent() {
    String domainName = "Test Domain";
    Filter filter = DomainUtils.buildNameAndParentDomainFilter(domainName, null);
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    // For root domains (null parent), should have 2 disjunctive criteria
    // (one for false, one for null hasParentDomain field)
    assertTrue(filter.getOr().size() >= 2);
    for (com.linkedin.metadata.query.filter.ConjunctiveCriterion criterion : filter.getOr()) {
      assertNotNull(criterion.getAnd());
      assertTrue(criterion.getAnd().size() > 0);
    }
  }

  @Test
  public void testBuildNameAndParentDomainFilterWithNameAndParent() {
    String domainName = "Child Domain";
    Urn parentUrn = UrnUtils.getUrn(PARENT_DOMAIN_URN);
    Filter filter = DomainUtils.buildNameAndParentDomainFilter(domainName, parentUrn);
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 1);
    com.linkedin.metadata.query.filter.ConjunctiveCriterion criterion = filter.getOr().get(0);
    assertNotNull(criterion.getAnd());
    // Should have: hasParentDomain=true criterion, parentDomain criterion, and name criterion
    assertTrue(criterion.getAnd().size() >= 3);
  }

  @Test
  public void testBuildNameAndParentDomainFilterWithNullNameAndParent() {
    Urn parentUrn = UrnUtils.getUrn(PARENT_DOMAIN_URN);
    Filter filter = DomainUtils.buildNameAndParentDomainFilter(null, parentUrn);
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 1);
    com.linkedin.metadata.query.filter.ConjunctiveCriterion criterion = filter.getOr().get(0);
    assertNotNull(criterion.getAnd());
    // Should have: hasParentDomain=true criterion and parentDomain criterion (no name)
    assertTrue(criterion.getAnd().size() >= 2);
  }

  @Test
  public void testBuildNameAndParentDomainFilterWithNullNameAndNullParent() {
    Filter filter = DomainUtils.buildNameAndParentDomainFilter(null, null);
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    // For root domains without name, should have 2 disjunctive criteria
    assertTrue(filter.getOr().size() >= 2);
    for (com.linkedin.metadata.query.filter.ConjunctiveCriterion criterion : filter.getOr()) {
      assertNotNull(criterion.getAnd());
      assertTrue(criterion.getAnd().size() > 0);
    }
  }

  @Test
  public void testBuildParentDomainFilterWithNullParent() {
    Filter filter = DomainUtils.buildParentDomainFilter(null);
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    // For root domains, should have 2 disjunctive criteria
    assertTrue(filter.getOr().size() >= 2);
  }

  @Test
  public void testBuildParentDomainFilterWithParent() {
    Urn parentUrn = UrnUtils.getUrn(PARENT_DOMAIN_URN);
    Filter filter = DomainUtils.buildParentDomainFilter(parentUrn);
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 1);
    com.linkedin.metadata.query.filter.ConjunctiveCriterion criterion = filter.getOr().get(0);
    assertNotNull(criterion.getAnd());
    // Should have at least: hasParentDomain and parentDomain criteria
    assertTrue(criterion.getAnd().size() >= 2);
  }

  @Test
  public void testHasNameConflictTrueWhenConflictExists() throws Exception {
    String domainName = "Existing Domain";
    Urn parentUrn = UrnUtils.getUrn(PARENT_DOMAIN_URN);
    QueryContext allowContext = getMockAllowContext();

    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(UrnUtils.getUrn(DOMAIN_URN));

    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray(searchEntity));

    Mockito.when(
            mockEntityClient.filter(
                any(), eq(DOMAIN_ENTITY_NAME), any(Filter.class), isNull(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    DomainProperties props = new DomainProperties();
    props.setName(domainName);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(DOMAIN_URN));
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(props.data()));
    aspectMap.put(DOMAIN_PROPERTIES_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(aspectMap);

    Mockito.when(mockEntityClient.batchGetV2(any(), eq(DOMAIN_ENTITY_NAME), any(), any()))
        .thenReturn(Collections.singletonMap(UrnUtils.getUrn(DOMAIN_URN), entityResponse));

    assertTrue(DomainUtils.hasNameConflict(domainName, parentUrn, allowContext, mockEntityClient));
  }

  @Test
  public void testHasNameConflictFalseWhenNoConflict() throws Exception {
    String domainName = "Unique Domain";
    QueryContext allowContext = getMockAllowContext();

    SearchResult emptySearchResult = new SearchResult();
    emptySearchResult.setEntities(new SearchEntityArray());

    Mockito.when(
            mockEntityClient.filter(
                any(), eq(DOMAIN_ENTITY_NAME), any(Filter.class), isNull(), anyInt(), anyInt()))
        .thenReturn(emptySearchResult);

    Mockito.when(mockEntityClient.batchGetV2(any(), eq(DOMAIN_ENTITY_NAME), any(), any()))
        .thenReturn(Collections.emptyMap());

    assertFalse(DomainUtils.hasNameConflict(domainName, null, allowContext, mockEntityClient));
  }

  @Test
  public void testGetParentDomainSafelyWithParent() {
    DomainProperties properties = new DomainProperties();
    Urn parentUrn = UrnUtils.getUrn(PARENT_DOMAIN_URN);
    properties.setParentDomain(parentUrn);

    Urn result = DomainUtils.getParentDomainSafely(properties);
    assertNotNull(result);
    assertEquals(result.toString(), PARENT_DOMAIN_URN);
  }

  @Test
  public void testGetParentDomainSafelyWithoutParent() {
    DomainProperties properties = new DomainProperties();
    Urn result = DomainUtils.getParentDomainSafely(properties);
    assertNull(result);
  }

  @Test
  public void testGetParentDomainSafelyWithNullProperties() {
    Urn result = DomainUtils.getParentDomainSafely(null);
    assertNull(result);
  }
}
