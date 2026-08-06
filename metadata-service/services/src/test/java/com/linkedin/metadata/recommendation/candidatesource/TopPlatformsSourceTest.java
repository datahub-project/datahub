package com.linkedin.metadata.recommendation.candidatesource;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Proves the TopPlatforms candidate source validates all platform candidates with a single batched
 * lookup rather than one lookup per candidate (the N+1 that used to live in {@code
 * isValidCandidateUrn}).
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

  /** Only the first {@code withLogoCount} platforms have a logo (i.e. are valid candidates). */
  private Set<Urn> stubPlatformAspects(int total, int withLogoCount) {
    Map<Urn, List<RecordTemplate>> aspects = new HashMap<>();
    for (int i = 0; i < total; i++) {
      Urn urn = new DataPlatformUrn("platform-" + i);
      DataPlatformInfo info = new DataPlatformInfo().setName("platform-" + i);
      if (i < withLogoCount) {
        info.setLogoUrl(new Url("http://logo/" + i));
      }
      aspects.put(urn, List.of(info));
    }
    Mockito.when(
            entityService.getLatestAspects(
                any(OperationContext.class),
                anySet(),
                eq(Set.of(DATA_PLATFORM_INFO_ASPECT_NAME)),
                eq(false)))
        .thenReturn(aspects);
    return aspects.entrySet().stream()
        .filter(e -> ((DataPlatformInfo) e.getValue().get(0)).hasLogoUrl())
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  @Test
  public void testValidationIsBatchedNotPerCandidate() {
    // 30 candidate platforms, 20 of which have logos.
    stubAggregation(30);
    Set<Urn> expectedValid = stubPlatformAspects(30, 20);

    List<RecommendationContent> results = source.getRecommendations(opContext, HOME, null);

    // Functional: only the platforms with logos are recommended.
    Set<Urn> recommended =
        results.stream().map(RecommendationContent::getEntity).collect(Collectors.toSet());
    assertEquals(recommended, expectedValid);
    assertEquals(results.size(), 20);

    // Performance: validation happened in exactly ONE batched call regardless of 30 candidates,
    // and the old per-candidate single-aspect fetch is never used.
    verify(entityService, times(1))
        .getLatestAspects(any(), anySet(), eq(Set.of(DATA_PLATFORM_INFO_ASPECT_NAME)), eq(false));
    verify(entityService, never()).getLatestAspect(any(), any(Urn.class), anyString());
  }

  @Test
  public void testCallCountIsConstantRegardlessOfCandidateCount() {
    // Small input.
    stubAggregation(3);
    stubPlatformAspects(3, 3);
    source.getRecommendations(opContext, HOME, null);
    verify(entityService, times(1))
        .getLatestAspects(any(), anySet(), eq(Set.of(DATA_PLATFORM_INFO_ASPECT_NAME)), eq(false));

    // Large input: still exactly one batched call — call count does not scale with N.
    clearInvocations(entityService);
    stubAggregation(30);
    stubPlatformAspects(30, 30);
    source.getRecommendations(opContext, HOME, null);
    verify(entityService, times(1))
        .getLatestAspects(any(), anySet(), eq(Set.of(DATA_PLATFORM_INFO_ASPECT_NAME)), eq(false));
    verify(entityService, never()).getLatestAspect(any(), any(Urn.class), anyString());
  }
}
