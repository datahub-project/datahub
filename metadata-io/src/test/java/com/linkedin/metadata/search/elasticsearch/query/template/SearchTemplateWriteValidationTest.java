package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.validation.SearchTemplateValidator;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class SearchTemplateWriteValidationTest extends SearchTemplateFixture {
  @Test
  public void testValidator() {
    SearchConfiguration search = new SearchConfiguration();
    SearchV3Configuration v3 = new SearchV3Configuration();
    v3.setTemplates(templates);
    search.setV3(v3);
    SearchTemplateValidator validator =
        new SearchTemplateValidator(loader, search)
            .setConfig(
                AspectPluginConfig.builder()
                    .className(SearchTemplateValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(
                        List.of("CREATE", "CREATE_ENTITY", "DELETE", "UPDATE", "UPSERT", "PATCH"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(SEARCH_TEMPLATE_ENTITY_NAME)
                                .aspectName("*")
                                .build()))
                    .build());
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(retriever)
            .build();

    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN),
                    USER,
                    ChangeType.UPSERT,
                    body("Fulltext", "{ \"match_all\": {} }", null)))
            .isEmpty());
    assertTrue(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Fulltext", "{ \"match_all\": {} }", null)))
            .isEmpty());
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN), SYSTEM, ChangeType.DELETE, null))
            .isEmpty());

    Urn custom = UrnUtils.getUrn("urn:li:searchTemplate:custom");
    SearchTemplateProperties locked = body("Locked", "{ \"match_all\": {} }", null);
    locked.setEditable(false);
    store(custom.toString(), locked);
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    custom, USER, ChangeType.UPSERT, body("Locked", "{ \"match_all\": {} }", null)))
            .isEmpty());

    SearchTemplateProperties child = body("Child", null, null);
    child.setParent(UrnUtils.getUrn("urn:li:searchTemplate:missing"));
    assertFalse(
        validate(validator, retrieverContext, item(custom, SYSTEM, ChangeType.UPSERT, child))
            .isEmpty());

    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    parent.setDialect(SearchTemplateDialect.POSTGRES);
    store("urn:li:searchTemplate:pgParent", parent);
    SearchTemplateProperties mismatch = body("Mismatch", "{ \"match_all\": {} }", null);
    mismatch.setParent(UrnUtils.getUrn("urn:li:searchTemplate:pgParent"));
    assertFalse(
        validate(validator, retrieverContext, item(custom, SYSTEM, ChangeType.UPSERT, mismatch))
            .isEmpty());

    store(SEARCH_TEMPLATE_LIST_URN, body("List", "{ \"match_all\": {} }", LIST_SCORE));
    SearchTemplateProperties inheritedRescore = body("Inherited rescore", null, null);
    inheritedRescore.setParent(UrnUtils.getUrn(SEARCH_TEMPLATE_LIST_URN));
    inheritedRescore.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} } } }");
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(custom, SYSTEM, ChangeType.UPSERT, inheritedRescore))
            .isEmpty());

    Urn patched = UrnUtils.getUrn("urn:li:searchTemplate:patched");
    assertTrue(
        validate(validator, retrieverContext, item(patched, SYSTEM, ChangeType.PATCH, null))
            .isEmpty());
    ChangeMCP applied =
        change(
            patched,
            body("Patched", "{ \"script\": { \"script\": { \"source\": \"1\" } } }", null));
    assertFalse(
        validator
            .validatePreCommit(opContext, List.of(applied), retrieverContext)
            .collect(Collectors.toList())
            .isEmpty());
  }

  @Test
  public void testDeleteRejectsARemainingChild() {
    String parent = "urn:li:searchTemplate:custom";
    String child = "urn:li:searchTemplate:child";
    store(parent, body("Custom", "{ \"match_all\": {} }", null));
    store(child, parentOf("Child", parent));
    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext = holeContext();
    Urn parentUrn = UrnUtils.getUrn(parent);
    assertTrue(
        validate(validator, retrieverContext, item(parentUrn, SYSTEM, ChangeType.DELETE, null))
            .stream()
            .anyMatch(exception -> exception.getMessage().contains("still a parent")));

    String alone = "urn:li:searchTemplate:alone";
    store(alone, body("Alone", "{ \"match_all\": {} }", null));
    assertTrue(
        validate(
                validator,
                retrieverContext,
                item(UrnUtils.getUrn(alone), SYSTEM, ChangeType.DELETE, null))
            .isEmpty());

    ChangeMCP deleteParent = item(parentUrn, SYSTEM, ChangeType.DELETE, null);
    ChangeMCP deleteChild = item(UrnUtils.getUrn(child), SYSTEM, ChangeType.DELETE, null);
    assertTrue(
        validator
            .validateProposed(opContext, List.of(deleteParent, deleteChild), retrieverContext, null)
            .findAny()
            .isEmpty());

    SearchTemplateProperties moved = parentOf("Child", alone);
    ChangeMCP reparent = item(UrnUtils.getUrn(child), SYSTEM, ChangeType.UPSERT, moved);
    assertTrue(
        validator
            .validateProposed(opContext, List.of(deleteParent, reparent), retrieverContext, null)
            .findAny()
            .isEmpty());

    SearchTemplateProperties cleared = body("Child", null, null);
    ChangeMCP clearParent = item(UrnUtils.getUrn(child), SYSTEM, ChangeType.UPSERT, cleared);
    assertTrue(
        validator
            .validateProposed(opContext, List.of(deleteParent, clearParent), retrieverContext, null)
            .findAny()
            .isEmpty());

    SearchTemplateProperties kept = parentOf("Child", parent);
    ChangeMCP keepParent = item(UrnUtils.getUrn(child), SYSTEM, ChangeType.UPSERT, kept);
    assertTrue(
        validator
            .validateProposed(opContext, List.of(deleteParent, keepParent), retrieverContext, null)
            .findAny()
            .isPresent());

    store("urn:li:searchTemplate:other", parentOf("Other", parent));
    assertTrue(
        validator
            .validateProposed(opContext, List.of(deleteParent, deleteChild), retrieverContext, null)
            .findAny()
            .isPresent());
  }

  @Test
  public void testSameBatchDeleteRejectsANewChild() {
    String parent = "urn:li:searchTemplate:custom";
    store(parent, body("Custom", "{ \"match_all\": {} }", null));
    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext = holeContext();
    ChangeMCP deleteParent = item(UrnUtils.getUrn(parent), SYSTEM, ChangeType.DELETE, null);
    ChangeMCP createChild =
        item(
            UrnUtils.getUrn("urn:li:searchTemplate:newChild"),
            SYSTEM,
            ChangeType.UPSERT,
            parentOf("New", parent));
    assertTrue(
        validator
            .validateProposed(opContext, List.of(deleteParent, createChild), retrieverContext, null)
            .anyMatch(exception -> exception.getMessage().contains("still a parent")));
    assertTrue(
        validator
            .validatePreCommit(opContext, List.of(deleteParent, createChild), retrieverContext)
            .anyMatch(exception -> exception.getMessage().contains("parent not found")));

    SearchTemplateProperties rewritten = body("Custom", "{ \"match_all\": {} }", null);
    ChangeMCP recreate = item(UrnUtils.getUrn(parent), SYSTEM, ChangeType.UPSERT, rewritten);
    assertTrue(
        validator
            .validateProposed(
                opContext, List.of(deleteParent, recreate, createChild), retrieverContext, null)
            .findAny()
            .isEmpty());
  }

  @Test
  public void testParentQueryKeepsHolesAnInheritingChildFills() {
    String parent = "urn:li:searchTemplate:custom";
    store(parent, body("Custom", HOLE_QUERY, null));
    SearchTemplateProperties child = parentOf("Child", parent);
    child.setQueryFragments(new StringMap(Map.of("extraDisMaxQueries", FILLED_HOLE)));
    store("urn:li:searchTemplate:child", child);
    SearchTemplateProperties grandchild = parentOf("Grandchild", "urn:li:searchTemplate:child");
    grandchild.setQueryFragments(new StringMap(Map.of("extraDisMaxQueries", FILLED_HOLE)));
    store("urn:li:searchTemplate:grandchild", grandchild);

    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext = holeContext();
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(parent),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Custom", "{ \"match_all\": {} }", null)))
            .isEmpty());
    assertTrue(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(parent),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Custom", HOLE_QUERY, null)))
            .isEmpty());
  }

  @Test
  public void testChildThatSetsQueryStopsTheHoleWalk() {
    String parent = "urn:li:searchTemplate:custom";
    store(parent, body("Custom", HOLE_QUERY, null));
    SearchTemplateProperties child = body("Child", "{ \"match_all\": {} }", null);
    child.setParent(UrnUtils.getUrn(parent));
    store("urn:li:searchTemplate:child", child);
    SearchTemplateProperties grandchild = parentOf("Grandchild", "urn:li:searchTemplate:child");
    grandchild.setQueryFragments(new StringMap(Map.of("missingHole", FILLED_HOLE)));
    store("urn:li:searchTemplate:grandchild", grandchild);
    assertTrue(
        validate(
                templateValidator(),
                holeContext(),
                item(
                    UrnUtils.getUrn(parent),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Custom", "{ \"match_all\": {} }", null)))
            .isEmpty());
  }

  @Test
  public void testScrollLeavesAnUnreadNextUnchecked() {
    String entry = "urn:li:searchTemplate:scrollEntry";
    SearchTemplateProperties properties = body("Scroll", "{ \"match_all\": {} }", null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setNext(UrnUtils.getUrn("urn:li:searchTemplate:missing-next"));
    properties.setLadder(ladder);
    store(entry, properties);
    templates.getDefaults().put("fulltext", Map.of("default", entry));
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            v3Config(),
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    assertThrows(
        SearchTemplateException.class, () -> coordinator.bind(opContext, "customer orders"));
    OperationContext scrolled = coordinator.bind(opContext, "customer orders", true);
    assertEquals(scrolled.getSearchContext().getResolvedSearchTemplate().getUrn(), entry);
    assertFalse(
        scrolled
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .contains("missing-next"));
  }

  @Test
  public void testWriteTimePlaceholdersUseRuntimeFieldPaths() {
    Map<String, Object> context = SearchTemplateMustache.validationContext("");
    assertEquals(context.get("nameKeyword"), SearchTemplateFields.keywordPath("name"));
    assertEquals(context.get("nameText"), SearchTemplateFields.textPath("name"));
    assertEquals(context.get("columnNameField"), SearchTemplateFields.keywordPath("name_1"));
    assertEquals(context.get("entityNameField"), SearchTemplateFields.keywordPath("name"));
    assertEquals(context.get("descriptionField"), SearchTemplateFields.textPath("description"));
    assertEquals(context.get("urnExactField"), "urn");
    assertEquals(context.get("urnFoldedField"), "urn.lowercase");
    assertEquals(context.get("urnParsedField"), "urn_name.lowercase");
    assertEquals(context.get("normalizedLastSegment"), "orders");
    assertFalse(String.valueOf(context.get("qualifiedNameKeyword")).contains("_search."));
    assertFalse(String.valueOf(context.get("idKeyword")).contains("_search."));
  }

  @Test
  public void testWriteTimeOverrideDoesNotAssumeRuntimeCatalog() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition defined = definition("certification.status", "string", null);
    DataHubSearchConfig config = new DataHubSearchConfig();
    config.setQueryByDefault(true);
    config.setBoostScore(3.0);
    defined.setSearchConfiguration(config);
    definitions.put(property, defined);
    SearchTemplateProperties template = body("Override", "{ \"match_all\": {} }", null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    SearchFieldBoost override = new SearchFieldBoost();
    override.setFieldName("structuredProperties.certification.status");
    override.setBoost(4.0);
    fields.setOverride(new SearchFieldBoostArray(override));
    template.setFields(fields);
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.check(
                template, 1024, opContext, retriever, List.of(TestEntitySpecBuilder.getSpec())));
  }

  @Test
  public void testProposedSkipsTheMergedRender() {
    String parent = "urn:li:searchTemplate:custom";
    store(parent, body("Custom", "{ not-json", null));
    SearchTemplateProperties child = parentOf("Child", parent);
    store("urn:li:searchTemplate:child", child);
    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext = holeContext();
    ChangeMCP change =
        item(UrnUtils.getUrn("urn:li:searchTemplate:child"), SYSTEM, ChangeType.UPSERT, child);
    assertTrue(
        validator
            .validateProposed(opContext, List.of(change), retrieverContext, null)
            .findAny()
            .isEmpty());
    assertFalse(
        validator
            .validatePreCommit(opContext, List.of(change), retrieverContext)
            .findAny()
            .isEmpty());
  }

  @Test
  public void testLastChildPageIsRevalidatedBeforeTheCap() {
    String over = "urn:li:searchTemplate:over";
    store(over, body("Over", "{ \"match_all\": {} }", null));
    inherit(over, "over", 104);
    String exact = "urn:li:searchTemplate:exact";
    store(exact, body("Exact", "{ \"match_all\": {} }", null));
    inherit(exact, "exact", 100);

    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext = pagedChildren();
    assertTrue(
        validate(validator, retrieverContext, queryWrite(over)).stream()
            .anyMatch(exception -> exception.getMessage().contains("more inheriting children")));
    assertTrue(
        validate(
                validator,
                retrieverContext,
                item(UrnUtils.getUrn(over), SYSTEM, ChangeType.DELETE, null))
            .stream()
            .anyMatch(exception -> exception.getMessage().contains("more inheriting children")));
    assertTrue(validate(validator, retrieverContext, queryWrite(exact)).isEmpty());
  }

  @Test
  public void testParallelMemberWithNextIsRejected() {
    String member = "urn:li:searchTemplate:member";
    SearchTemplateProperties memberBody = body("Member", "{ \"match_all\": {} }", null);
    memberBody.setLadder(stopLadder(1, "urn:li:searchTemplate:fallback"));
    store(member, memberBody);
    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext = retrieverContext();
    assertTrue(
        validate(validator, retrieverContext, queryWrite("urn:li:searchTemplate:group", member))
            .stream()
            .anyMatch(exception -> exception.getMessage().contains("must be one query")));

    String leaf = "urn:li:searchTemplate:leaf";
    SearchTemplateProperties leafBody = body("Leaf", "{ \"match_all\": {} }", null);
    leafBody.setLadder(stopLadder(1, null));
    store(leaf, leafBody);
    assertTrue(
        validate(validator, retrieverContext, queryWrite("urn:li:searchTemplate:group", leaf))
            .isEmpty());
  }

  @Test
  public void testInheritedLadderCycleIsRejected() {
    String left = "urn:li:searchTemplate:left";
    String inherited = "urn:li:searchTemplate:inherited";
    String parent = "urn:li:searchTemplate:ladderParent";
    SearchTemplateProperties parentBody = body("Parent", "{ \"match_all\": {} }", null);
    parentBody.setLadder(stopLadder(1, left));
    store(parent, parentBody);
    store(inherited, parentOf("Inherited", parent));
    SearchTemplateProperties writing = body("Left", "{ \"match_all\": {} }", null);
    writing.setLadder(stopLadder(1, inherited));
    assertTrue(
        validate(
                templateValidator(),
                retrieverContext(),
                item(UrnUtils.getUrn(left), SYSTEM, ChangeType.UPSERT, writing))
            .stream()
            .anyMatch(exception -> exception.getMessage().contains("ladder cycle")));
  }

  private static ChangeMCP queryWrite(String urn) {
    return item(
        UrnUtils.getUrn(urn),
        SYSTEM,
        ChangeType.UPSERT,
        body("Written", "{ \"match_all\": {} }", null));
  }

  private static ChangeMCP queryWrite(String urn, String member) {
    SearchTemplateProperties group = body("Group", "{ \"match_all\": {} }", null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setParallel(new UrnArray(UrnUtils.getUrn(member)));
    ladder.setJoin("any");
    group.setLadder(ladder);
    return item(UrnUtils.getUrn(urn), SYSTEM, ChangeType.UPSERT, group);
  }

  private void inherit(String parent, String prefix, int count) {
    for (int i = 0; i < count; i++) {
      store(String.format("urn:li:searchTemplate:%s%03d", prefix, i), parentOf("Child", parent));
    }
  }

  private RetrieverContext pagedChildren() {
    return RetrieverContext.builder()
        .graphRetriever(GraphRetriever.EMPTY)
        .searchRetriever(fullPageScroll())
        .cachingAspectRetriever(retriever)
        .build();
  }

  private SearchRetriever fullPageScroll() {
    return (entities, filters, scrollId, count, sortCriteria, searchFlags) -> {
      String parent = parentFilter(filters);
      List<Urn> matches = new ArrayList<>();
      if (parent != null) {
        for (Map.Entry<Urn, SearchTemplateProperties> entry : stored.entrySet()) {
          SearchTemplateProperties properties = entry.getValue();
          if (properties.hasParent()
              && properties.getParent() != null
              && parent.equals(properties.getParent().toString())) {
            matches.add(entry.getKey());
          }
        }
      }
      matches.sort(Comparator.comparing(Urn::toString));
      int start = scrollId == null ? 0 : Integer.parseInt(scrollId);
      int end = Math.min(start + 100, matches.size());
      SearchEntityArray found = new SearchEntityArray();
      for (Urn urn : matches.subList(start, end)) {
        found.add(new SearchEntity().setEntity(urn));
      }
      ScrollResult result = new ScrollResult();
      result.setEntities(found);
      result.setNumEntities(matches.size());
      result.setPageSize(found.size());
      if (found.size() == 100) {
        result.setScrollId(Integer.toString(end));
      }
      return result;
    };
  }
}
