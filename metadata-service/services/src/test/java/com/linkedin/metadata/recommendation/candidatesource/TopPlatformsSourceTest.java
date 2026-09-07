package com.linkedin.metadata.recommendation.candidatesource;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavior of the home-page "Platforms" candidate source. The source must NOT drop platforms that
 * lack a logo (regression guard: a platform that was searchable but had no dataPlatformInfo/logoUrl
 * was previously hidden from the Platforms module). It surfaces the top platforms by asset count,
 * capped at {@link TopPlatformsSource#getMaxContent()}.
 */
public class TopPlatformsSourceTest {

  private final EntityService<?> entityService = mock(EntityService.class);
  private final EntitySearchService entitySearchService = mock(EntitySearchService.class);
  private final EntityRegistry entityRegistry = mock(EntityRegistry.class);

  private static final Urn USER = new CorpuserUrn("test");
  private static final RecommendationRequestContext HOME =
      new RecommendationRequestContext().setScenario(ScenarioType.HOME);

  private OperationContext opContext;
  private TopPlatformsSource source;

  @BeforeMethod
  public void setup() {
    Mockito.reset(entityService, entitySearchService);
    opContext = TestOperationContexts.userContextNoSearchAuthorization(USER);
    source = new TopPlatformsSource(entitySearchService, entityService, entityRegistry);
    // Every candidate platform exists (the only validation the source applies).
    Mockito.when(entityService.exists(any(OperationContext.class), anySet(), anyBoolean()))
        .thenAnswer(invocation -> invocation.getArgument(1));
  }

  /** Aggregation returns {@code total} platform urns (platform-0 .. platform-{total-1}). */
  private void stubAggregation(int total) {
    Map<String, Long> agg = new LinkedHashMap<>();
    for (int i = 0; i < total; i++) {
      agg.put(new DataPlatformUrn("platform-" + i).toString(), (long) (total - i));
    }
    Mockito.when(
            entitySearchService.aggregateByValue(
                any(OperationContext.class), any(), eq("platform"), any(), anyInt()))
        .thenReturn(agg);
  }

  @Test
  public void testPlatformsWithoutLogosAreStillRecommended() {
    // 5 platforms with assets; none of them are gated on having a logoUrl.
    stubAggregation(5);

    List<RecommendationContent> results = source.getRecommendations(opContext, HOME, null);

    Set<Urn> recommended =
        results.stream().map(RecommendationContent::getEntity).collect(Collectors.toSet());
    Set<Urn> expected =
        Set.of(
            new DataPlatformUrn("platform-0"),
            new DataPlatformUrn("platform-1"),
            new DataPlatformUrn("platform-2"),
            new DataPlatformUrn("platform-3"),
            new DataPlatformUrn("platform-4"));
    assertEquals(recommended, expected);
    // The source must not fetch dataPlatformInfo just to gate on logoUrl.
    verify(entityService, never()).getLatestAspects(any(), anySet(), any(), anyBoolean());
  }

  @Test
  public void testRecommendationsAreCappedAtMaxContent() {
    // More platforms than the cap: only the top getMaxContent() by count are returned.
    stubAggregation(source.getMaxContent() + 10);

    List<RecommendationContent> results = source.getRecommendations(opContext, HOME, null);

    assertEquals(results.size(), source.getMaxContent());
    // Highest-count platform (platform-0) is kept.
    Set<Urn> recommended =
        results.stream().map(RecommendationContent::getEntity).collect(Collectors.toSet());
    assertTrue(recommended.contains(new DataPlatformUrn("platform-0")));
  }
}
