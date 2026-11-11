package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateScoreDefaults.EntityWeight;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateScoreDefaults.Extent;
import com.linkedin.metadata.search.elasticsearch.query.v3.TemplateConfiguredScores;
import com.linkedin.metadata.search.elasticsearch.query.v3.TemplateConfiguredScores.ResolvedFieldValueScore;
import com.linkedin.search.template.EntityTypeBoost;
import com.linkedin.search.template.EntityTypeBoostArray;
import com.linkedin.search.template.FieldFactor;
import com.linkedin.search.template.FieldFactorArray;
import com.linkedin.search.template.FieldValueScore;
import com.linkedin.search.template.FieldValueScoreArray;
import com.linkedin.search.template.TermWeight;
import com.linkedin.search.template.TermWeightArray;
import com.linkedin.structured.StructuredPropertyScore;
import java.util.List;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class SearchTemplateScoreDefaultsTest {
  @Test
  public void emptyEntryIsRejected() {
    expect(
        "names a field or a structured property",
        () -> SearchTemplateScoreDefaults.checkFieldValueScores(array(new FieldValueScore())));
  }

  @Test
  public void rawFieldWithoutExtentsIsRejected() {
    FieldValueScore missing = new FieldValueScore();
    missing.setField("usageCount");
    expect(
        "requires inputMin, inputMax, and maxMultiplier",
        () -> SearchTemplateScoreDefaults.checkFieldValueScores(array(missing)));

    FieldValueScore complete = new FieldValueScore();
    complete.setField("usageCount");
    complete.setInputMin(0.0);
    complete.setInputMax(4.0);
    complete.setMaxMultiplier(3.0);
    SearchTemplateScoreDefaults.checkFieldValueScores(array(complete));
    Extent extent = SearchTemplateScoreDefaults.extents(complete, null);
    assertEquals(extent.inputMin(), 0.0);
    assertEquals(extent.inputMax(), 4.0);
    assertEquals(extent.maxMultiplier(), 3.0);
  }

  @Test
  public void propertyEntryKeepsOmittedExtentsUnsetUntilRender() {
    FieldValueScore score = property("urn:li:structuredProperty:io.example.score");
    SearchTemplateScoreDefaults.checkFieldValueScores(array(score));
    assertFalse(score.hasInputMin());
    assertFalse(score.hasInputMax());
    assertFalse(score.hasMaxMultiplier());
    assertEquals(SearchTemplateScoreDefaults.extents(score, null), null);
  }

  @Test
  public void renderFillsOmittedExtentsFromTheDefinition() {
    FieldValueScore score = property("urn:li:structuredProperty:io.example.score");
    StructuredPropertyScore defined = new StructuredPropertyScore();
    defined.setInputMin(0.0);
    defined.setInputMax(10.0);
    defined.setMaxMultiplier(2.0);
    Extent filled = SearchTemplateScoreDefaults.extents(score, defined);
    assertEquals(filled.inputMin(), 0.0);
    assertEquals(filled.inputMax(), 10.0);
    assertEquals(filled.maxMultiplier(), 2.0);

    score.setInputMin(1.0);
    score.setMaxMultiplier(4.0);
    Extent overridden = SearchTemplateScoreDefaults.extents(score, defined);
    assertEquals(overridden.inputMin(), 1.0);
    assertEquals(overridden.inputMax(), 10.0);
    assertEquals(overridden.maxMultiplier(), 4.0);
  }

  @Test
  public void definitionWithoutAUsableScoreAddsNoMultiplier() {
    FieldValueScore score = property("urn:li:structuredProperty:io.example.score");
    assertEquals(SearchTemplateScoreDefaults.extents(score, null), null);
    StructuredPropertyScore partial = new StructuredPropertyScore();
    partial.setInputMin(0.0);
    partial.setMaxMultiplier(2.0);
    assertEquals(SearchTemplateScoreDefaults.extents(score, partial), null);
    String rendered =
        TemplateConfiguredScores.multiplyFieldValues(QueryBuilders.matchAllQuery(), List.of())
            .toString();
    assertFalse(rendered.contains("function_score"), rendered);
  }

  @Test
  public void fieldAndPropertyTogetherAreRejected() {
    FieldValueScore score = new FieldValueScore();
    score.setField("usageCount");
    score.setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:io.example.score"));
    try {
      SearchTemplateScoreDefaults.checkFieldValueScores(array(score));
      throw new AssertionError("expected one name");
    } catch (SearchTemplateException expected) {
      assertTrue(expected.getMessage().contains("not both"));
    }
  }

  @Test
  public void propertyUrnMustBeAStructuredProperty() {
    FieldValueScore score = new FieldValueScore();
    score.setPropertyUrn(UrnUtils.getUrn("urn:li:corpuser:someone"));
    try {
      SearchTemplateScoreDefaults.checkFieldValueScores(array(score));
      throw new AssertionError("expected a structured property URN");
    } catch (SearchTemplateException expected) {
      assertTrue(expected.getMessage().contains("structuredProperty"));
    }
  }

  @Test
  public void entityBoostWithoutATypeOrAWeightIsRejected() {
    expect(
        "requires an entity type",
        () -> SearchTemplateScoreDefaults.checkEntityBoosts(entityBoosts(new EntityTypeBoost())));
    EntityTypeBoost named = new EntityTypeBoost();
    named.setEntityType("glossaryTerm");
    expect(
        "weight must be positive",
        () -> SearchTemplateScoreDefaults.checkEntityBoosts(entityBoosts(named)));
  }

  @Test
  public void explicitTypeAndWeightMultiplyUntilTheRequestNamesOne() {
    EntityTypeBoostArray boosts = entityBoosts(entityBoost("glossaryTerm", 10.0));
    List<EntityWeight> weights = SearchTemplateScoreDefaults.entityBoosts(boosts, 2);
    assertEquals(weights.size(), 1);
    assertEquals(weights.get(0).entityType(), "glossaryTerm");
    assertEquals(weights.get(0).weight(), 10.0f);
    assertTrue(SearchTemplateScoreDefaults.entityBoosts(boosts, 1).isEmpty());
    assertTrue(
        SearchTemplateScoreDefaults.entityBoosts(entityBoosts(entityBoost("dataset", 1.0)), 2)
            .isEmpty());
    String rendered =
        TemplateConfiguredScores.entityBoosts(QueryBuilders.matchAllQuery(), weights).toString();
    assertTrue(rendered.contains("glossaryTerm"), rendered);
  }

  @Test
  public void namedValuesMultiply() {
    String rendered =
        TemplateConfiguredScores.multiplyFieldValues(
                QueryBuilders.matchAllQuery(),
                List.of(
                    new ResolvedFieldValueScore("usageCount", 0.0, 1.0, 1.5),
                    new ResolvedFieldValueScore("structuredProperties.two", 0.0, 1.0, 2.0)))
            .toString();
    assertTrue(rendered.contains("usageCount"), rendered);
    assertTrue(rendered.contains("structuredProperties.two"), rendered);
    assertTrue(
        rendered.indexOf("function_score") != rendered.lastIndexOf("function_score"), rendered);
  }

  @Test
  public void fieldFactorAndTermWeightMustNameOneSource() {
    FieldFactor both = new FieldFactor();
    both.setField("usageCount");
    both.setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:io.example.score"));
    expect("not both", () -> SearchTemplateScoreDefaults.checkFieldFactors(factors(both)));

    FieldFactor neither = new FieldFactor();
    expect(
        "names a field or a structured property",
        () -> SearchTemplateScoreDefaults.checkFieldFactors(factors(neither)));

    TermWeight badValue = term("deprecated", "true", 0.5);
    badValue.setValue("{\"nested\":true}");
    expect("one JSON value", () -> SearchTemplateScoreDefaults.checkTermWeights(weights(badValue)));

    TermWeight penalty = term("deprecated", "true", 0.0);
    expect("positive", () -> SearchTemplateScoreDefaults.checkTermWeights(weights(penalty)));

    FieldFactor negative = new FieldFactor();
    negative.setField("usageCount");
    negative.setFactor(-1.0);
    expect("positive", () -> SearchTemplateScoreDefaults.checkFieldFactors(factors(negative)));

    FieldFactor negativeMissing = new FieldFactor();
    negativeMissing.setField("usageCount");
    negativeMissing.setMissing(-1.0);
    expect(
        "at least 0",
        () -> SearchTemplateScoreDefaults.checkFieldFactors(factors(negativeMissing)));
  }

  @Test
  public void factorsThenTermWeightsFollowTheInheritedFunctions() throws Exception {
    ObjectNode factor =
        SearchTemplateJson.fieldFactorFunction("combinedSearchRankingMultiplier", 1.0, 1.0, null);
    ObjectNode term =
        SearchTemplateJson.termWeightFunction(
            "deprecated", SearchTemplateMappers.JSON.readTree("true"), 0.5);
    String rendered =
        SearchTemplateJson.wrapFunctionScore(
                QueryBuilders.matchAllQuery(),
                "{ \"functions\": [ { \"weight\": 1.0 } ], \"score_mode\": \"avg\","
                    + " \"boost_mode\": \"multiply\" }",
                List.of(),
                new SearchTemplateJson.FunctionScoreTail(List.of(factor, term), "multiply", null))
            .toString();
    int holes = rendered.indexOf("\"weight\"");
    int multiplier = rendered.indexOf("combinedSearchRankingMultiplier");
    int deprecated = rendered.indexOf("deprecated");
    assertTrue(holes >= 0 && holes < multiplier && multiplier < deprecated, rendered);
    assertFalse(rendered.contains("\"avg\""), rendered);
    assertTrue(factor.toString().contains("\"gte\":0"), factor.toString());
    assertTrue(rendered.contains("include_lower"), rendered);

    String reciprocal =
        SearchTemplateJson.fieldFactorFunction("usageCount", 1.0, null, "reciprocal").toString();
    assertTrue(reciprocal.contains("\"gt\":0"), reciprocal);
    assertFalse(reciprocal.contains("exists"), reciprocal);

    String square =
        SearchTemplateJson.fieldFactorFunction("usageCount", 1.0, null, "square").toString();
    assertFalse(square.contains("range"), square);
  }

  @Test
  public void entityWeightsMultiplyTheQuery() {
    String rendered =
        TemplateConfiguredScores.entityBoosts(
                QueryBuilders.matchAllQuery(), List.of(new EntityWeight("glossaryTerm", 10.0f)))
            .toString();
    assertTrue(rendered.contains("function_score"), rendered);
    assertTrue(rendered.contains("glossaryTerm"), rendered);
    assertFalse(rendered.contains("dataset"));
  }

  private static void expect(String message, Runnable check) {
    try {
      check.run();
      throw new AssertionError("expected rejection");
    } catch (SearchTemplateException expected) {
      assertTrue(expected.getMessage().contains(message), expected.getMessage());
    }
  }

  private static FieldFactorArray factors(FieldFactor factor) {
    FieldFactorArray factors = new FieldFactorArray();
    factors.add(factor);
    return factors;
  }

  private static TermWeightArray weights(TermWeight weight) {
    TermWeightArray weights = new TermWeightArray();
    weights.add(weight);
    return weights;
  }

  private static TermWeight term(String field, String value, double weight) {
    TermWeight term = new TermWeight();
    term.setField(field);
    term.setValue(value);
    term.setWeight(weight);
    return term;
  }

  private static FieldValueScoreArray array(FieldValueScore score) {
    FieldValueScoreArray scores = new FieldValueScoreArray();
    scores.add(score);
    return scores;
  }

  private static FieldValueScore property(String urn) {
    FieldValueScore score = new FieldValueScore();
    score.setPropertyUrn(UrnUtils.getUrn(urn));
    return score;
  }

  private static EntityTypeBoostArray entityBoosts(EntityTypeBoost boost) {
    EntityTypeBoostArray boosts = new EntityTypeBoostArray();
    boosts.add(boost);
    return boosts;
  }

  private static EntityTypeBoost entityBoost(String entityType, double weight) {
    EntityTypeBoost boost = new EntityTypeBoost();
    boost.setEntityType(entityType);
    boost.setWeight(weight);
    return boost;
  }
}
