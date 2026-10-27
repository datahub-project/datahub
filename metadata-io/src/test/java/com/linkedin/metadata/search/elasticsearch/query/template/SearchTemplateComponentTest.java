package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.search.template.SearchTemplateComponentMode;
import com.linkedin.search.template.SearchTemplateProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/** Registry lookup, mode rules, skip conditions, the subset invariant, and the cache key. */
public class SearchTemplateComponentTest {
  @Test
  public void testRegistryLookup() {
    SearchTemplateComponentRegistry registry =
        new SearchTemplateComponentRegistry(List.of(new ReverseComponent()));
    assertTrue(registry.find("reverse", 1).isPresent());
    assertFalse(registry.find("reverse", 2).isPresent());
    expectThrows(SearchTemplateException.class, () -> registry.require("missing", 1));
    expectThrows(
        IllegalStateException.class,
        () ->
            new SearchTemplateComponentRegistry(
                List.of(new ReverseComponent(), new ReverseComponent())));
  }

  @Test
  public void testOwnRungRejectsQuerySectionsAndPostProcessKeepsThem() {
    SearchTemplateComponentRegistry registry =
        new SearchTemplateComponentRegistry(List.of(new ReverseComponent()));
    SearchTemplateProperties own = componentProperties(SearchTemplateComponentMode.OWN_RUNG);
    own.setQuery("{\"match_all\": {}}");
    expectThrows(
        SearchTemplateException.class, () -> SearchTemplateComponents.checkWrite(own, registry));

    SearchTemplateProperties post = componentProperties(SearchTemplateComponentMode.POST_PROCESS);
    post.setQuery("{\"match_all\": {}}");
    SearchTemplateComponents.checkWrite(post, registry);

    SearchTemplateProperties bad = componentProperties(SearchTemplateComponentMode.POST_PROCESS);
    bad.getComponent().getConfig().put("bad", "1");
    expectThrows(
        SearchTemplateException.class, () -> SearchTemplateComponents.checkWrite(bad, registry));
    expectThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateComponents.checkWrite(
                componentProperties(SearchTemplateComponentMode.POST_PROCESS),
                SearchTemplateComponentRegistry.empty()));
  }

  @Test
  public void testSkipConditions() {
    assertTrue(SearchTemplateComponents.skip(true, 10, null, 0, 50));
    assertTrue(SearchTemplateComponents.skip(false, 0, null, 0, 50));
    assertTrue(
        SearchTemplateComponents.skip(
            false,
            10,
            List.of(new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING)),
            0,
            50));
    assertTrue(SearchTemplateComponents.skip(false, 10, null, 50, 50));
    assertFalse(SearchTemplateComponents.skip(false, 10, null, 0, 50));
    assertFalse(
        SearchTemplateComponents.skip(
            false,
            10,
            List.of(new SortCriterion().setField("_score").setOrder(SortOrder.DESCENDING)),
            0,
            50));
  }

  @Test
  public void testPostProcessReordersSubsetAndKeepsTotal() {
    SearchTemplateComponentRegistry registry =
        new SearchTemplateComponentRegistry(List.of(new ReverseComponent()));
    SearchEntity first = hit("urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)");
    SearchEntity second = hit("urn:li:dataset:(urn:li:dataPlatform:hive,b,PROD)");
    SearchResult page = new SearchResult();
    page.setEntities(new SearchEntityArray(List.of(first, second)));
    page.setFrom(0);
    page.setPageSize(2);
    page.setNumEntities(9);
    com.linkedin.search.template.SearchTemplateComponent spec =
        componentProperties(SearchTemplateComponentMode.POST_PROCESS).getComponent();
    MergedSearchTemplate template =
        MergedSearchTemplate.builder()
            .urn("urn:li:searchTemplate:ranked")
            .dialect("ELASTICSEARCH")
            .queryFragments(Map.of())
            .parameters(Map.of())
            .chain(List.of("urn:li:searchTemplate:ranked"))
            .component(spec)
            .build();
    SearchTemplateComponentRequest request =
        SearchTemplateComponentRequest.builder()
            .query("orders")
            .from(0)
            .size(2)
            .hits(List.of(first, second))
            .total(9)
            .config(SearchTemplateComponents.config(spec))
            .mode(SearchTemplateComponentMode.POST_PROCESS)
            .build();
    SearchTemplateComponents.applyPostProcess(registry, template, request, page);
    assertEquals(page.getNumEntities(), 9);
    assertEquals(page.getEntities().get(0).getEntity(), second.getEntity());
    assertEquals(page.getEntities().get(1).getEntity(), first.getEntity());
  }

  @Test
  public void testPostProcessRejectsHitsOutsideTheWindow() {
    SearchEntity first = hit("urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)");
    SearchEntity extra = hit("urn:li:dataset:(urn:li:dataPlatform:hive,extra,PROD)");
    expectThrows(
        SearchTemplateException.class,
        () -> SearchTemplateComponents.enforceSubset(List.of(first), List.of(first, extra)));
    expectThrows(
        SearchTemplateException.class,
        () -> SearchTemplateComponents.enforceSubset(List.of(first), List.of(first, first)));
  }

  @Test
  public void testCacheKeyIncludesComponentIdentity() {
    com.linkedin.search.template.SearchTemplateComponent spec =
        componentProperties(SearchTemplateComponentMode.POST_PROCESS).getComponent();
    String suffix = SearchTemplateComponents.cacheSuffix(spec);
    assertTrue(suffix.contains("reverse"));
    assertTrue(suffix.contains("POST_PROCESS"));
    assertTrue(suffix.contains("window=10"));
    assertEquals(
        "",
        SearchTemplateComponents.cacheSuffix(
            new com.linkedin.search.template.SearchTemplateComponent()));
  }

  private static SearchTemplateProperties componentProperties(SearchTemplateComponentMode mode) {
    com.linkedin.search.template.SearchTemplateComponent component =
        new com.linkedin.search.template.SearchTemplateComponent();
    component.setName("reverse");
    component.setVersion(1);
    component.setMode(mode);
    component.setConfig(new StringMap(Map.of("window", "10")));
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName("ranked");
    properties.setComponent(component);
    return properties;
  }

  private static SearchEntity hit(String urn) {
    return new SearchEntity().setEntity(UrnUtils.getUrn(urn));
  }

  /** Test double. Reverses the fetched window and only accepts POST_PROCESS. */
  private static final class ReverseComponent implements SearchTemplateComponent {
    @Override
    public String name() {
      return "reverse";
    }

    @Override
    public int version() {
      return 1;
    }

    @Override
    public void validate(SearchTemplateComponentMode mode, Map<String, String> config) {
      if (mode != SearchTemplateComponentMode.POST_PROCESS) {
        throw new SearchTemplateException("reverse is POST_PROCESS only");
      }
      if (config.containsKey("bad")) {
        throw new SearchTemplateException("bad config");
      }
    }

    @Override
    public int plan(SearchTemplateComponentRequest request, Map<String, String> config) {
      return Integer.parseInt(config.getOrDefault("window", "10"));
    }

    @Override
    public SearchTemplateComponentPage execute(SearchTemplateComponentRequest request) {
      List<SearchEntity> reversed = new ArrayList<>(request.getHits());
      java.util.Collections.reverse(reversed);
      return new SearchTemplateComponentPage(
          reversed, request.getTotal(), request.getMetadata(), List.of(), false, false, null);
    }
  }
}
