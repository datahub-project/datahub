package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.StructuredPropertyClause;
import com.linkedin.search.template.StructuredPropertyQuery;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import org.testng.annotations.Test;

public class SearchTemplateStructuredClauseTest extends SearchTemplateFixture {
  @Test
  public void testStructuredPropertyUrnConstantAndKeyword() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    definitions.put(property, definition("certification.status", "string", null));
    SearchTemplateProperties template =
        body(
            "Property",
            "{ \"term\": { \"{{urn:li:structuredProperty:certification.status}}.keyword\": \"gold\" } }",
            null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    OperationContext bound =
        new SearchTemplateCoordinator(
                v3Config(),
                null,
                loader,
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty())
            .bind(opContext, "customer orders");
    String rendered =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    assertTrue(rendered.contains("structuredProperties.certification_status"));
    assertFalse(rendered.contains("structuredProperties.certification_status.keyword"));
    assertFalse(rendered.contains("urn:li:structuredProperty:"));
  }

  @Test
  public void testStructuredPropertyVersionChangesTheField() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    definitions.put(property, definition("certification.status", "string", "00000000000001"));
    SearchTemplateProperties template =
        body(
            "Versioned",
            "{ \"term\": { \"{{urn:li:structuredProperty:certification.status}}\": \"gold\" } }",
            null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    OperationContext bound =
        new SearchTemplateCoordinator(
                v3Config(),
                null,
                loader,
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty())
            .bind(opContext, "customer orders");
    String rendered =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    assertTrue(rendered.contains("structuredProperties._versioned.certification_status."));
    assertTrue(rendered.contains("00000000000001"));
  }

  @Test
  public void testUnknownStructuredPropertyAndIllegalKeywordAreRejected() {
    SearchTemplateProperties unknown =
        body(
            "Missing",
            "{ \"term\": { \"{{urn:li:structuredProperty:missing}}\": \"gold\" } }",
            null);
    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplateChecks.check(unknown, 1024, opContext, retriever, null));

    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    definitions.put(property, definition("priority", "number", null));
    SearchTemplateProperties numbered = body("Number", "{ \"match_all\": {} }", null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    SearchFieldBoost boost = new SearchFieldBoost();
    boost.setFieldName(property.toString());
    boost.setSubfield(StructuredPropertyReferences.KEYWORD);
    boost.setBoost(2.0);
    fields.setAdd(new SearchFieldBoostArray(boost));
    numbered.setFields(fields);
    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplateChecks.check(numbered, 1024, opContext, retriever, null));
  }

  @Test
  public void testStructuredPropertyLogicalNameResolvesToTheIndexedPath() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    definitions.put(property, definition("certification.status", "string", null));
    assertEquals(
        StructuredPropertyReferences.resolveMember(
            "structuredProperties.certification.status", null, opContext, retriever),
        "structuredProperties.certification_status");

    definitions.put(property, definition("certification.status", "string", "00000000000001"));
    String versioned =
        StructuredPropertyReferences.resolveMember(
            "structuredProperties.certification.status", null, opContext, retriever);
    assertTrue(versioned.startsWith("structuredProperties._versioned."));
    assertTrue(versioned.contains("certification_status"));
  }

  @Test
  public void testQueryByDefaultStringPropertyIsATerm() {
    Urn stringProperty = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn numberProperty = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    Urn urnProperty = UrnUtils.getUrn("urn:li:structuredProperty:owner");
    StructuredPropertyDefinition stringDefinition =
        definition("certification.status", "string", null);
    DataHubSearchConfig stringConfig = new DataHubSearchConfig();
    stringConfig.setQueryByDefault(true);
    stringConfig.setBoostScore(3.0);
    stringDefinition.setSearchConfiguration(stringConfig);
    StructuredPropertyDefinition numberDefinition = definition("priority", "number", null);
    DataHubSearchConfig numberConfig = new DataHubSearchConfig();
    numberConfig.setQueryByDefault(true);
    numberDefinition.setSearchConfiguration(numberConfig);
    StructuredPropertyDefinition urnDefinition = definition("owner", "urn", null);
    DataHubSearchConfig urnConfig = new DataHubSearchConfig();
    urnConfig.setQueryByDefault(true);
    urnDefinition.setSearchConfiguration(urnConfig);
    definitions.put(stringProperty, stringDefinition);
    definitions.put(numberProperty, numberDefinition);
    definitions.put(urnProperty, urnDefinition);
    SearchTemplateProperties fulltext = body("Fulltext", FULLTEXT_QUERY, null);
    StructuredPropertyQuery values = new StructuredPropertyQuery();
    values.setAnalyzedValue(new StructuredPropertyClause());
    values.setExactValue(new StructuredPropertyClause());
    fulltext.setStructuredPropertyQuery(values);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, fulltext);
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchTemplateRenderer renderer =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null);
    String rendered =
        renderer
            .render(bound, List.of(registry.getEntitySpec(DATASET_ENTITY_NAME)), "gold", true)
            .toString();
    assertTrue(rendered.contains("structuredProperties.certification_status"), rendered);
    assertFalse(rendered.contains("structuredProperties.certification_status.keyword"), rendered);
    assertTrue(rendered.contains("gold"), rendered);
    assertFalse(rendered.contains("structuredProperties.certification_status^"), rendered);
    assertFalse(rendered.contains("structuredProperties.priority"), rendered);
    assertTrue(rendered.contains("structuredProperties.owner__urn_name"), rendered);
    assertFalse(rendered.contains("structuredProperties.owner.keyword"), rendered);

    String phrase =
        renderer
            .render(
                bound, List.of(registry.getEntitySpec(DATASET_ENTITY_NAME)), "gold certified", true)
            .toString();
    assertTrue(phrase.contains("gold certified"), phrase);
  }

  @Test
  public void testStructuredPropertyQueryInheritsUntilCleared() {
    String parentUrn = "urn:li:searchTemplate:parentNameValue";
    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    parent.setStructuredPropertyQuery(nameAndValue(8f));
    store(parentUrn, parent);

    SearchTemplateProperties child = parentOf("Child", parentUrn);
    MergedSearchTemplate inherited =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childNameValue", child);
    assertEquals(inherited.getStructuredPropertyQuery().getNameAndValue().getWeight(), 8f);

    child.setStructuredPropertyQuery(nameAndValue(4f));
    MergedSearchTemplate replaced =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childNameValue", child);
    assertEquals(replaced.getStructuredPropertyQuery().getNameAndValue().getWeight(), 4f);

    child.setStructuredPropertyQuery(new StructuredPropertyQuery());
    MergedSearchTemplate dropped =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childNameValue", child);
    assertNull(dropped.getStructuredPropertyQuery());
  }

  @Test
  public void testNameAndValueWeightMustBePositive() {
    SearchTemplateProperties empty = body("Empty", "{ \"match_all\": {} }", null);
    empty.setStructuredPropertyQuery(new StructuredPropertyQuery());
    SearchTemplateChecks.check(empty, 1024);

    SearchTemplateProperties positive = body("Positive", "{ \"match_all\": {} }", null);
    positive.setStructuredPropertyQuery(nameAndValue(8f));
    SearchTemplateChecks.check(positive, 1024);

    SearchTemplateProperties zero = body("Zero", "{ \"match_all\": {} }", null);
    zero.setStructuredPropertyQuery(nameAndValue(0f));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(zero, 1024));

    SearchTemplateProperties negative = body("Negative", "{ \"match_all\": {} }", null);
    negative.setStructuredPropertyQuery(nameAndValue(-1f));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(negative, 1024));
  }

  @Test
  public void testNameAndValueExamples() {
    installCertification(null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, nameValueTemplate(null));
    String gold = renderStored("gold", false);
    assertTrue(gold.contains("structuredProperties.certification_status"), gold);
    assertFalse(gold.contains("structuredProperties.certification_status.keyword"), gold);
    assertTrue(gold.contains("gold"), gold);
    assertFalse(gold.contains("dis_max"), gold);

    String named = renderStored("certification status gold", false);
    assertTrue(named.contains("dis_max"), named);
    assertTrue(named.contains("\"gold\""), named);
    assertTrue(named.contains("structuredProperties.certification_status"), named);
    assertFalse(named.contains("structuredProperties.certification_status.keyword"), named);
    assertTrue(named.contains("certification status gold"), named);

    String silver = renderStored("certification status silver", false);
    assertFalse(silver.contains("gold"), silver);
    assertTrue(silver.contains("\"silver\""), silver);

    String nameOnly = renderStored("certification status", false);
    assertFalse(nameOnly.contains("dis_max"), nameOnly);

    String trailing = renderStored("status gold", false);
    assertFalse(trailing.contains("dis_max"), trailing);

    String qualified = renderStored("certification.status gold", false);
    assertTrue(qualified.contains("dis_max"), qualified);
    assertTrue(qualified.contains("\"gold\""), qualified);
  }

  @Test
  public void testRemovingAPropertyDropsBothClauses() {
    Urn property = installCertification(null);
    SearchTemplateProperties template = nameValueTemplate(null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    fields.setRemove(new StringArray(property.toString()));
    template.setFields(fields);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    String named = renderStored("certification status gold", false);
    String value = renderStored("gold", false);
    assertFalse(named.contains("structuredProperties.certification_status"), named);
    assertFalse(named.contains("dis_max"), named);
    assertFalse(value.contains("structuredProperties.certification_status"), value);
  }

  @Test
  public void testOverrideDoesNotChangeNameAndValueWeight() {
    Urn property = installCertification(null);
    SearchTemplateProperties template = nameValueTemplate(null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    SearchFieldBoost override = new SearchFieldBoost();
    override.setFieldName(property.toString());
    override.setBoost(4.0);
    fields.setOverride(new SearchFieldBoostArray(override));
    template.setFields(fields);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    String named = renderStored("certification status gold", false);
    assertTrue(named.contains("\"boost\" : 8.0"), named);
    assertTrue(named.contains("\"boost\" : 4.0"), named);
  }

  @Test
  public void testNameAndValueSkipsDateNumberAndUrn() {
    Urn notes = UrnUtils.getUrn("urn:li:structuredProperty:notes");
    Urn priority = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    Urn due = UrnUtils.getUrn("urn:li:structuredProperty:due");
    Urn owner = UrnUtils.getUrn("urn:li:structuredProperty:owner");
    StructuredPropertyDefinition notesDefinition =
        searchableProperty("notes", "rich_text", "Notes");
    StructuredPropertyDefinition priorityDefinition =
        searchableProperty("priority", "number", "Priority");
    StructuredPropertyDefinition dueDefinition = searchableProperty("due", "date", "Due Date");
    StructuredPropertyDefinition ownerDefinition = searchableProperty("owner", "urn", "Owner");
    definitions.put(notes, notesDefinition);
    definitions.put(priority, priorityDefinition);
    definitions.put(due, dueDefinition);
    definitions.put(owner, ownerDefinition);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, nameValueTemplate(null));
    String rich = renderStored("notes gold", false);
    assertTrue(rich.contains("dis_max"), rich);
    assertTrue(rich.contains("structuredProperties.notes"), rich);
    assertFalse(rich.contains("structuredProperties.notes.keyword"), rich);
    assertTrue(rich.contains("\"gold\""), rich);

    String numbered = renderStored("priority gold", false);
    assertFalse(numbered.contains("dis_max"), numbered);
    assertFalse(numbered.contains("structuredProperties.priority"), numbered);

    String dated = renderStored("due date gold", false);
    assertFalse(dated.contains("dis_max"), dated);
    assertFalse(dated.contains("structuredProperties.due"), dated);

    String urned = renderStored("owner gold", false);
    assertFalse(urned.contains("dis_max"), urned);
    assertFalse(urned.contains("\"gold\""), urned);
  }

  @Test
  public void testNameAndValueDisMaxSitsInsideFunctionScore() {
    installCertification(null);
    store(
        SEARCH_TEMPLATE_FULLTEXT_URN,
        nameValueTemplate(
            "{ \"functions\": [], \"score_mode\": \"multiply\", \"boost_mode\": \"multiply\" }"));
    String rendered = renderStored("certification status gold", false);
    int functionScore = rendered.indexOf("\"function_score\"");
    int disMax = rendered.indexOf("\"dis_max\"");
    assertTrue(functionScore >= 0, rendered);
    assertTrue(disMax > functionScore, rendered);
    assertTrue(rendered.contains("\"tie_breaker\" : 0.0"), rendered);
  }

  @Test
  public void testNameAndValueStaysOnScroll() {
    installCertification(null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, nameValueTemplate(null));
    String rendered = renderStored("certification status gold", true);
    assertTrue(rendered.contains("dis_max"), rendered);
    assertTrue(rendered.contains("\"gold\""), rendered);
  }
}
