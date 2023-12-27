package com.linkedin.metadata.recommendation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.TestEntityUtil;
import com.linkedin.metadata.recommendation.candidatesource.TestSource;
import com.linkedin.metadata.recommendation.ranker.RecommendationModuleRanker;
import com.linkedin.metadata.recommendation.ranker.SimpleRecommendationRanker;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class RecommendationsServiceTest {

  private final TestSource nonEligibleSource =
      new TestSource(
          "not eligible",
          "nonEligible",
          RecommendationRenderType.ENTITY_NAME_LIST,
          false,
          getContentFromString(ImmutableList.of("test")));
  private final TestSource emptySource =
      new TestSource(
          "empty", "empty", RecommendationRenderType.ENTITY_NAME_LIST, true, ImmutableList.of());
  private final TestSource valuesSource =
      new TestSource(
          "values",
          "values",
          RecommendationRenderType.ENTITY_NAME_LIST,
          true,
          getContentFromString(ImmutableList.of("test")));
  private final TestSource multiValuesSource =
      new TestSource(
          "multiValues",
          "multiValues",
          RecommendationRenderType.ENTITY_NAME_LIST,
          true,
          getContentFromString(ImmutableList.of("test1", "test2", "test3", "test4")));
  private final TestSource urnsSource =
      new TestSource(
          "urns",
          "urns",
          RecommendationRenderType.ENTITY_NAME_LIST,
          true,
          getContentFromUrns(ImmutableList.of(TestEntityUtil.getTestEntityUrn())));
  private final TestSource multiUrnsSource =
      new TestSource(
          "multiUrns",
          "multiUrns",
          RecommendationRenderType.ENTITY_NAME_LIST,
          true,
          getContentFromUrns(
              ImmutableList.of(
                  TestEntityUtil.getTestEntityUrn(),
                  TestEntityUtil.getTestEntityUrn(),
                  TestEntityUtil.getTestEntityUrn())));
  private final RecommendationModuleRanker ranker = new SimpleRecommendationRanker();

  private List<RecommendationContent> getContentFromString(List<String> values) {
    return values.stream()
        .map(value -> new RecommendationContent().setValue(value))
        .collect(Collectors.toList());
  }

  private List<RecommendationContent> getContentFromUrns(List<Urn> urns) {
    return urns.stream()
        .map(urn -> new RecommendationContent().setValue(urn.toString()).setEntity(urn))
        .collect(Collectors.toList());
  }

  @Test
  public void testService() throws URISyntaxException {
    // Test non-eligible and empty
    RecommendationsService service =
        new RecommendationsService(ImmutableList.of(nonEligibleSource, emptySource), ranker);
    List<RecommendationModule> result =
        service.listRecommendations(
            Urn.createFromString("urn:li:corpuser:me"),
            new RecommendationRequestContext().setScenario(ScenarioType.HOME),
            10);
    assertTrue(result.isEmpty());

    // Test empty with one valid source
    service =
        new RecommendationsService(
            ImmutableList.of(nonEligibleSource, emptySource, valuesSource), ranker);
    result =
        service.listRecommendations(
            Urn.createFromString("urn:li:corpuser:me"),
            new RecommendationRequestContext().setScenario(ScenarioType.HOME),
            10);
    assertEquals(result.size(), 1);
    RecommendationModule module = result.get(0);
    assertEquals(module.getTitle(), "values");
    assertEquals(module.getModuleId(), "values");
    assertEquals(module.getRenderType(), RecommendationRenderType.ENTITY_NAME_LIST);
    assertEquals(module.getContent(), valuesSource.getContents());

    // Test multiple sources
    service =
        new RecommendationsService(
            ImmutableList.of(valuesSource, multiValuesSource, urnsSource, multiUrnsSource), ranker);
    result =
        service.listRecommendations(
            Urn.createFromString("urn:li:corpuser:me"),
            new RecommendationRequestContext().setScenario(ScenarioType.HOME),
            10);
    assertEquals(result.size(), 4);
    module = result.get(0);
    assertEquals(module.getTitle(), "values");
    assertEquals(module.getModuleId(), "values");
    assertEquals(module.getRenderType(), RecommendationRenderType.ENTITY_NAME_LIST);
    assertEquals(module.getContent(), valuesSource.getContents());
    module = result.get(1);
    assertEquals(module.getTitle(), "multiValues");
    assertEquals(module.getModuleId(), "multiValues");
    assertEquals(module.getRenderType(), RecommendationRenderType.ENTITY_NAME_LIST);
    assertEquals(module.getContent(), multiValuesSource.getContents());
    module = result.get(2);
    assertEquals(module.getTitle(), "urns");
    assertEquals(module.getModuleId(), "urns");
    assertEquals(module.getRenderType(), RecommendationRenderType.ENTITY_NAME_LIST);
    assertEquals(module.getContent(), urnsSource.getContents());
    module = result.get(3);
    assertEquals(module.getTitle(), "multiUrns");
    assertEquals(module.getModuleId(), "multiUrns");
    assertEquals(module.getRenderType(), RecommendationRenderType.ENTITY_NAME_LIST);
    assertEquals(module.getContent(), multiUrnsSource.getContents());

    // Test limit
    result =
        service.listRecommendations(
            Urn.createFromString("urn:li:corpuser:me"),
            new RecommendationRequestContext().setScenario(ScenarioType.HOME),
            2);
    assertEquals(result.size(), 2);
    module = result.get(0);
    assertEquals(module.getTitle(), "values");
    assertEquals(module.getModuleId(), "values");
    assertEquals(module.getRenderType(), RecommendationRenderType.ENTITY_NAME_LIST);
    assertEquals(module.getContent(), valuesSource.getContents());
    module = result.get(1);
    assertEquals(module.getTitle(), "multiValues");
    assertEquals(module.getModuleId(), "multiValues");
    assertEquals(module.getRenderType(), RecommendationRenderType.ENTITY_NAME_LIST);
    assertEquals(module.getContent(), multiValuesSource.getContents());
  }
}
