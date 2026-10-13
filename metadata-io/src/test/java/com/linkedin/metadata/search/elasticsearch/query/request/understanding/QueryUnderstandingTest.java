package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import static org.testng.Assert.assertEquals;

import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class QueryUnderstandingTest {

  @Test
  public void testIdentityUrn() {
    assertEquals(
        QueryUnderstanding.understand(
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.orders,PROD)"),
        QueryIntent.IDENTITY);
  }

  @Test
  public void testIdentityS3() {
    assertEquals(
        QueryUnderstanding.understand("s3://bucket/path/to/dataset"), QueryIntent.IDENTITY);
  }

  @Test
  public void testIdentityGcs() {
    assertEquals(QueryUnderstanding.understand("gs://bucket/path"), QueryIntent.IDENTITY);
  }

  @Test
  public void testIdentityHdfs() {
    assertEquals(QueryUnderstanding.understand("hdfs://cluster/path"), QueryIntent.IDENTITY);
  }

  @Test
  public void testFqnThreeSegments() {
    assertEquals(QueryUnderstanding.understand("my_db.sales.orders"), QueryIntent.FQN);
  }

  @Test
  public void testFqnTwoSegments() {
    assertEquals(QueryUnderstanding.understand("analytics_db.dim_user"), QueryIntent.FQN);
  }

  @Test
  public void testFqnAllCaps() {
    assertEquals(
        QueryUnderstanding.understand("MY_DB.MY_SCHEMA.PAGE_VIEW_EVENTS"), QueryIntent.FQN);
  }

  @Test
  public void testExactNameSingleToken() {
    assertEquals(QueryUnderstanding.understand("wau"), QueryIntent.EXACT_NAME);
  }

  @Test
  public void testExactNameWithUnderscore() {
    assertEquals(QueryUnderstanding.understand("dim_user"), QueryIntent.EXACT_NAME);
  }

  @Test
  public void testExactNameMultipleUnderscores() {
    assertEquals(QueryUnderstanding.understand("orders_by_day"), QueryIntent.EXACT_NAME);
  }

  @Test
  public void testKeywordMultiWord() {
    assertEquals(QueryUnderstanding.understand("user engagement"), QueryIntent.KEYWORD);
  }

  @Test
  public void testKeywordDescriptive() {
    assertEquals(QueryUnderstanding.understand("conversion rate by region"), QueryIntent.KEYWORD);
  }

  @Test
  public void testEmptyQuery() {
    assertEquals(QueryUnderstanding.understand(""), QueryIntent.KEYWORD);
  }

  @Test
  public void testWildcardStar() {
    assertEquals(QueryUnderstanding.understand("*"), QueryIntent.KEYWORD);
  }

  @Test
  public void testQuotedPhrase() {
    assertEquals(QueryUnderstanding.understand("\"exact phrase\""), QueryIntent.EXACT_NAME);
  }

  @Test
  public void testSingleQuotedPhrase() {
    assertEquals(QueryUnderstanding.understand("'exact phrase'"), QueryIntent.EXACT_NAME);
  }

  @Test
  public void testDottedTwoSegments() {
    assertEquals(QueryUnderstanding.understand("user.facts"), QueryIntent.FQN);
  }

  @Test
  public void testHyphenatedName() {
    assertEquals(QueryUnderstanding.understand("covid-19-data"), QueryIntent.EXACT_NAME);
  }

  // --- Synonym normalization tests ---

  private static final Map<String, Set<String>> TEST_SYNONYMS =
      Map.of(
          "monetisation", Set.of("monetisation", "monetization"),
          "monetization", Set.of("monetisation", "monetization"),
          "organisation", Set.of("organisation", "organization"),
          "organization", Set.of("organisation", "organization"));

  @Test
  public void testNormalizeSynonymVariant() {
    // Both spellings should normalize to the same canonical form
    assertEquals(
        QueryUnderstanding.normalizeSynonyms("ad monetisation", TEST_SYNONYMS),
        QueryUnderstanding.normalizeSynonyms("ad monetization", TEST_SYNONYMS));
  }

  @Test
  public void testNormalizeSynonymCanonicalForm() {
    // Canonical = alphabetically first = "monetisation"
    assertEquals(
        QueryUnderstanding.normalizeSynonyms("ad monetization", TEST_SYNONYMS), "ad monetisation");
  }

  @Test
  public void testNormalizeNoSynonym() {
    // Tokens not in synonym map are unchanged
    assertEquals(
        QueryUnderstanding.normalizeSynonyms("data warehouse", TEST_SYNONYMS), "data warehouse");
  }

  @Test
  public void testNormalizeMultipleSynonyms() {
    assertEquals(
        QueryUnderstanding.normalizeSynonyms("monetization organization", TEST_SYNONYMS),
        "monetisation organisation");
  }

  @Test
  public void testNormalizeNullMap() {
    assertEquals(QueryUnderstanding.normalizeSynonyms("monetization", null), "monetization");
  }

  @Test
  public void testNormalizeEmptyQuery() {
    assertEquals(QueryUnderstanding.normalizeSynonyms("", TEST_SYNONYMS), "");
  }

  @Test
  public void testNormalizeSkipsSemanticSynonyms() {
    // "arr" → "annual recurring revenue" is a semantic expansion, not a spelling variant.
    // Normalization should NOT replace it.
    Map<String, Set<String>> semanticSyns =
        Map.of(
            "arr", Set.of("arr", "annual recurring revenue"),
            "annual recurring revenue", Set.of("arr", "annual recurring revenue"));
    assertEquals(QueryUnderstanding.normalizeSynonyms("arr", semanticSyns), "arr");
  }

  @Test
  public void testNormalizeSkipsAbbreviationSynonyms() {
    // "conv" → "conversion" more than two edits apart, skip normalization
    Map<String, Set<String>> abbrSyns =
        Map.of(
            "conv", Set.of("conv", "conversion", "conversions"),
            "conversion", Set.of("conv", "conversion", "conversions"),
            "conversions", Set.of("conv", "conversion", "conversions"));
    assertEquals(
        QueryUnderstanding.normalizeSynonyms("conversion rate", abbrSyns), "conversion rate");
  }

  @Test
  public void testNormalizeSkipsRelatedNames() {
    // Single words of similar length that are not spellings of one another
    Map<String, Set<String>> related =
        Map.of("glue", Set.of("glue", "athena"), "athena", Set.of("glue", "athena"));
    assertEquals(QueryUnderstanding.normalizeSynonyms("glue", related), "glue");
  }

  @Test
  public void testAnalyzeReturnsBothIntentAndNormalized() {
    QueryUnderstanding.Result result = QueryUnderstanding.analyze("ad monetization", TEST_SYNONYMS);
    assertEquals(result.intent(), QueryIntent.KEYWORD);
    assertEquals(result.normalizedQuery(), "ad monetisation");
  }
}
