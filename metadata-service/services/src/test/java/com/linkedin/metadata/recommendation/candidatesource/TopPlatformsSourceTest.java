package com.linkedin.metadata.recommendation.candidatesource;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

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
import java.util.stream.IntStream;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavior of the home-page "Platforms" candidate source. The source must NOT drop platforms that
 * lack a logo (regression guard: a platform that was searchable but had no dataPlatformInfo/logoUrl
 * was previously hidden from the Platforms module). It surfaces platforms ranked by asset count,
 * capped at {@link TopPlatformsSource#getMaxContent()}, and drops candidates that do not exist.
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
    // By default every candidate platform exists.
    Mockito.when(entityService.exists(any(OperationContext.class), anySet(), anyBoolean()))
        .thenAnswer(invocation -> invocation.getArgument(1));
  }

  private static Urn platform(int i) {
    return new DataPlatformUrn("platform-" + i);
  }

  /** Aggregation returns {@code total} platform urns with descending counts (platform-0 highest). */
  private void stubAggregation(int total) {
    Map<String, Long> agg = new LinkedHashMap<>();
    for (int i = 0; i < total; i++) {
      agg.put(platform(i).toString(), (long) (total - i));
    }
    Mockito.when(
            entitySearchService.aggregateByValue(
                any(OperationContext.class), any(), eq("platform"), any(), anyInt()))
        .thenReturn(agg);
  }

  private List<Urn> recommendedPlatforms() {
    return source.getRecommendations(opContext, HOME, null).stream()
        .map(RecommendationContent::getEntity)
        .collect(Collectors.toList());
  }

  @Test
  public void testPlatformsWithoutLogosAreRecommendedInRankOrder() {
    // 5 platforms with assets; none are gated on having a logoUrl, and rank order is preserved.
    stubAggregation(5);

    assertEquals(
        recommendedPlatforms(),
        List.of(platform(0), platform(1), platform(2), platform(3), platform(4)));
    // The source must not fetch dataPlatformInfo just to gate on logoUrl.
    verify(entityService, never()).getLatestAspects(any(), anySet(), any(), anyBoolean());
  }

  @Test
  public void testRecommendationsAreCappedAtMaxContentInRankOrder() {
    // More platforms than the cap: the top getMaxContent() by count are kept, highest first.
    stubAggregation(source.getMaxContent() + 10);

    List<Urn> recommended = recommendedPlatforms();
    assertEquals(recommended.size(), source.getMaxContent());
    assertEquals(
        recommended,
        IntStream.range(0, source.getMaxContent())
            .mapToObj(TopPlatformsSourceTest::platform)
            .collect(Collectors.toList()));
  }

  @Test
  public void testNonExistentPlatformsAreFilteredOut() {
    // Only a subset of the aggregated platforms actually exist; only those are recommended.
    stubAggregation(5);
    Set<Urn> existing = Set.of(platform(0), platform(2), platform(4));
    Mockito.when(entityService.exists(any(OperationContext.class), anySet(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              Set<Urn> candidates = invocation.getArgument(1);
              return candidates.stream().filter(existing::contains).collect(Collectors.toSet());
            });

    assertEquals(recommendedPlatforms(), List.of(platform(0), platform(2), platform(4)));
  }
}
