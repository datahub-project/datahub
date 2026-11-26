package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.search.hybrid.HybridLexicalScoreNormalizer;
import com.linkedin.metadata.search.hybrid.HybridScoreCombiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The {@code knn} blend read with {@code retrieval.combine: keywordRerank}. The defaults are the
 * hybrid reranker's: a vector weight of 0.5 and a lexical sigmoid over 0 to 500 with steepness 6.
 * {@code model} and {@code field} are the deprecated names of the embedding profile id. {@code
 * minScore} in the section is not read; the request flag of that name is a separate gate.
 *
 * @param blendFloor minimum normalized vector score, 0 to 1, for a row to be blended. Null when
 *     unset. A row below it keeps its keyword position.
 * @param others what happens to rows that are not rescored. {@code keepKeywordPlaces} is the only
 *     value.
 */
public record SearchTemplateRerankKnn(
    @Nullable String model,
    @Nullable String field,
    double vectorWeight,
    double lexicalInputMin,
    double lexicalInputMax,
    double lexicalSteepness,
    @Nullable Double blendFloor,
    @Nonnull String others) {

  public static final String KEEP_KEYWORD_PLACES = "keepKeywordPlaces";
  public static final double DEFAULT_VECTOR_WEIGHT = HybridScoreCombiner.DEFAULT_VECTOR_WEIGHT;
  public static final double DEFAULT_LEXICAL_INPUT_MIN =
      HybridLexicalScoreNormalizer.DEFAULT_INPUT_MIN;
  public static final double DEFAULT_LEXICAL_INPUT_MAX =
      HybridLexicalScoreNormalizer.DEFAULT_INPUT_MAX;
  public static final double DEFAULT_LEXICAL_STEEPNESS =
      HybridLexicalScoreNormalizer.DEFAULT_STEEPNESS;
}
