package com.linkedin.metadata.recommendation.candidatesource;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntitySearchAggregationCandidateSourceTest {
  private final EntitySearchService entitySearchService = mock(EntitySearchService.class);
  private final EntityRegistry entityRegistry = mock(EntityRegistry.class);
  private EntitySearchAggregationSource valueBasedCandidateSource;
  private EntitySearchAggregationSource urnBasedCandidateSource;
  private OperationContext opContext;
  private Filter filter;

  private static final Urn USER = new CorpuserUrn("test");
  private static final RecommendationRequestContext CONTEXT =
      new RecommendationRequestContext().setScenario(ScenarioType.HOME);

  @BeforeMethod
  public void setup() {
    opContext = TestOperationContexts.userContextNoSearchAuthorization(USER);
    Mockito.reset(entitySearchService);
    valueBasedCandidateSource = buildCandidateSource("testValue", false);
    urnBasedCandidateSource = buildCandidateSource("testUrn", true);
    filter = new Filter();
  }

  private EntitySearchAggregationSource buildCandidateSource(
      String identifier, boolean isValueUrn) {
    return new EntitySearchAggregationSource(entitySearchService, entityRegistry) {
      @Override
      protected String getSearchFieldName() {
        return identifier;
      }

      @Override
      protected int getMaxContent() {
        return 2;
      }

      @Override
      protected boolean isValueUrn() {
        return isValueUrn;
      }

      @Override
      public String getTitle() {
        return identifier;
      }

      @Override
      public String getModuleId() {
        return identifier;
      }

      @Override
      public RecommendationRenderType getRenderType() {
        return RecommendationRenderType.ENTITY_NAME_LIST;
      }

      @Override
      public boolean isEligible(
          @Nonnull OperationContext opContext,
          @Nonnull RecommendationRequestContext requestContext) {
        return true;
      }
    };
  }

  @Test
  public void testWhenSearchServiceReturnsEmpty() {
    Mockito.when(
            entitySearchService.aggregateByValue(
                any(OperationContext.class),
                eq(Collections.emptyList()),
                eq("testValue"),
                same(filter),
                anyInt()))
        .thenReturn(Collections.emptyMap());

    List<RecommendationContent> candidates =
        valueBasedCandidateSource.getRecommendations(opContext, CONTEXT, filter);
    assertTrue(candidates.isEmpty());
    assertFalse(
        valueBasedCandidateSource.getRecommendationModule(opContext, CONTEXT, filter).isPresent());

    // Mockito's default stub could also return an empty map.
    // Adding explicit verification to guard against this.
    verify(entitySearchService, times(2))
        .aggregateByValue(
            any(OperationContext.class),
            eq(Collections.emptyList()),
            eq("testValue"),
            same(filter),
            anyInt());
  }

  @Test
  public void testWhenSearchServiceReturnsValueResults() {
    // One result
    Mockito.when(
            entitySearchService.aggregateByValue(
                any(OperationContext.class), any(), eq("testValue"), same(filter), anyInt()))
        .thenReturn(ImmutableMap.of("value1", 1L));
    List<RecommendationContent> candidates =
        valueBasedCandidateSource.getRecommendations(opContext, CONTEXT, filter);
    assertEquals(candidates.size(), 1);
    RecommendationContent content = candidates.get(0);
    assertEquals(content.getValue(), "value1");
    assertNull(content.getEntity());
    RecommendationParams params = content.getParams();
    assertNotNull(params);
    assertNotNull(params.getSearchParams());
    assertTrue(StringUtils.isEmpty(params.getSearchParams().getQuery()));
    assertEquals(params.getSearchParams().getFilters().size(), 1);
    assertEquals(
        params.getSearchParams().getFilters().get(0),
        buildCriterion("testValue", Condition.EQUAL, "value1"));
    assertNotNull(params.getContentParams());
    assertEquals(params.getContentParams().getCount().longValue(), 1L);
    assertTrue(
        valueBasedCandidateSource.getRecommendationModule(opContext, CONTEXT, filter).isPresent());

    // Multiple result
    Mockito.when(
            entitySearchService.aggregateByValue(
                any(OperationContext.class), any(), eq("testValue"), same(filter), anyInt()))
        .thenReturn(ImmutableMap.of("value1", 1L, "value2", 2L, "value3", 3L));
    candidates = valueBasedCandidateSource.getRecommendations(opContext, CONTEXT, filter);
    assertEquals(candidates.size(), 2);
    content = candidates.get(0);
    assertEquals(content.getValue(), "value3");
    assertNull(content.getEntity());
    params = content.getParams();
    assertNotNull(params);
    assertNotNull(params.getSearchParams());
    assertTrue(StringUtils.isEmpty(params.getSearchParams().getQuery()));
    assertEquals(params.getSearchParams().getFilters().size(), 1);
    assertEquals(
        params.getSearchParams().getFilters().get(0),
        buildCriterion("testValue", Condition.EQUAL, "value3"));
    assertNotNull(params.getContentParams());
    assertEquals(params.getContentParams().getCount().longValue(), 3L);
    content = candidates.get(1);
    assertEquals(content.getValue(), "value2");
    assertNull(content.getEntity());
    params = content.getParams();
    assertNotNull(params);
    assertNotNull(params.getSearchParams());
    assertTrue(StringUtils.isEmpty(params.getSearchParams().getQuery()));
    assertEquals(params.getSearchParams().getFilters().size(), 1);
    assertEquals(
        params.getSearchParams().getFilters().get(0),
        buildCriterion("testValue", Condition.EQUAL, "value2"));
    assertNotNull(params.getContentParams());
    assertEquals(params.getContentParams().getCount().longValue(), 2L);
    assertTrue(
        valueBasedCandidateSource.getRecommendationModule(opContext, CONTEXT, filter).isPresent());
  }

  @Test
  public void testWhenSearchServiceReturnsUrnResults() {
    // One result
    Urn testUrn1 = new TestEntityUrn("testUrn1", "testUrn1", "testUrn1");
    Urn testUrn2 = new TestEntityUrn("testUrn2", "testUrn2", "testUrn2");
    Urn testUrn3 = new TestEntityUrn("testUrn3", "testUrn3", "testUrn3");
    Mockito.when(
            entitySearchService.aggregateByValue(
                any(OperationContext.class), any(), eq("testUrn"), same(filter), anyInt()))
        .thenReturn(ImmutableMap.of(testUrn1.toString(), 1L));
    List<RecommendationContent> candidates =
        urnBasedCandidateSource.getRecommendations(opContext, CONTEXT, filter);
    assertEquals(candidates.size(), 1);
    RecommendationContent content = candidates.get(0);
    assertEquals(content.getValue(), testUrn1.toString());
    assertEquals(content.getEntity(), testUrn1);
    RecommendationParams params = content.getParams();
    assertNotNull(params);
    assertNotNull(params.getSearchParams());
    assertTrue(StringUtils.isEmpty(params.getSearchParams().getQuery()));
    assertEquals(params.getSearchParams().getFilters().size(), 1);
    assertEquals(
        params.getSearchParams().getFilters().get(0),
        buildCriterion("testUrn", Condition.EQUAL, testUrn1.toString()));
    assertNotNull(params.getContentParams());
    assertEquals(params.getContentParams().getCount().longValue(), 1L);
    assertTrue(
        urnBasedCandidateSource.getRecommendationModule(opContext, CONTEXT, filter).isPresent());

    // Multiple result
    Mockito.when(
            entitySearchService.aggregateByValue(
                any(OperationContext.class), any(), eq("testUrn"), same(filter), anyInt()))
        .thenReturn(
            ImmutableMap.of(
                testUrn1.toString(), 1L, testUrn2.toString(), 2L, testUrn3.toString(), 3L));
    candidates = urnBasedCandidateSource.getRecommendations(opContext, CONTEXT, filter);
    assertEquals(candidates.size(), 2);
    content = candidates.get(0);
    assertEquals(content.getValue(), testUrn3.toString());
    assertEquals(content.getEntity(), testUrn3);
    params = content.getParams();
    assertNotNull(params);
    assertNotNull(params.getSearchParams());
    assertTrue(StringUtils.isEmpty(params.getSearchParams().getQuery()));
    assertEquals(params.getSearchParams().getFilters().size(), 1);
    assertEquals(
        params.getSearchParams().getFilters().get(0),
        buildCriterion("testUrn", Condition.EQUAL, testUrn3.toString()));
    assertNotNull(params.getContentParams());
    assertEquals(params.getContentParams().getCount().longValue(), 3L);
    content = candidates.get(1);
    assertEquals(content.getValue(), testUrn2.toString());
    assertEquals(content.getEntity(), testUrn2);
    params = content.getParams();
    assertNotNull(params);
    assertNotNull(params.getSearchParams());
    assertTrue(StringUtils.isEmpty(params.getSearchParams().getQuery()));
    assertEquals(params.getSearchParams().getFilters().size(), 1);
    assertEquals(
        params.getSearchParams().getFilters().get(0),
        buildCriterion("testUrn", Condition.EQUAL, testUrn2.toString()));
    assertNotNull(params.getContentParams());
    assertEquals(params.getContentParams().getCount().longValue(), 2L);
    assertTrue(
        urnBasedCandidateSource.getRecommendationModule(opContext, CONTEXT, filter).isPresent());
  }
}
