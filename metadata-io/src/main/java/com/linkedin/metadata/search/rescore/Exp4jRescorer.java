package com.linkedin.metadata.search.rescore;

import com.linkedin.metadata.search.rescore.functions.BooleanFunction;
import com.linkedin.metadata.search.rescore.functions.LinearDecayFunction;
import com.linkedin.metadata.search.rescore.functions.SigmoidFunction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.opensearch.search.SearchHit;

/**
 * Java-based rescorer using exp4j for formula evaluation.
 *
 * <p>This replaces the Elasticsearch Painless script-based rescoring with a Java implementation
 * that provides full explainability.
 *
 * <p>Key features: - Parses and compiles exp4j formula once, reuses for all documents - Extracts
 * signals from ES documents and normalizes them - Evaluates formula and returns detailed
 * explanation - Thread-safe via expression copying
 */
@Slf4j
public class Exp4jRescorer {

  private final Expression compiledExpression;
  private final String formula;
  private final List<SignalDefinition> signals;
  private final SignalExtractor signalExtractor;

  /**
   * Create a new rescorer with the given formula and signal definitions.
   *
   * @param formula exp4j formula string (e.g., "pow(norm_bm25, 1.0) * pow(norm_views, 0.8)")
   * @param signals list of signal definitions
   */
  public Exp4jRescorer(String formula, List<SignalDefinition> signals) {
    Objects.requireNonNull(formula, "formula cannot be null");
    Objects.requireNonNull(signals, "signals cannot be null");

    this.formula = formula;
    // Defensive copy to prevent external modification
    this.signals = signals.isEmpty() ? Collections.emptyList() : List.copyOf(signals);
    this.signalExtractor = new SignalExtractor();

    // Build expression with custom functions and variables
    ExpressionBuilder builder =
        new ExpressionBuilder(formula)
            .functions(new SigmoidFunction(), new LinearDecayFunction(), new BooleanFunction());

    // Add all signal variables
    for (SignalDefinition signal : signals) {
      builder.variable(signal.getNormalizedName());
    }

    this.compiledExpression = builder.build();

    log.info(
        "Initialized Exp4jRescorer with formula: {} and {} signals",
        formula.substring(0, Math.min(50, formula.length())),
        signals.size());
  }

  /**
   * Rescore a single search hit.
   *
   * @param hit the Elasticsearch search hit
   * @param esScore the _score from Elasticsearch (BM25 + boosts)
   * @return rescore result with final score and explanation
   */
  public RescoreResult rescore(SearchHit hit, double esScore) {
    // Extract and normalize all signals
    Map<String, SignalValue> signalValues = signalExtractor.extract(hit, esScore, signals);

    // Copy expression for thread safety
    Expression localExpression = new Expression(compiledExpression);

    // Set all variable values
    for (SignalDefinition signal : signals) {
      SignalValue sv = signalValues.get(signal.getName());
      if (sv != null) {
        localExpression.setVariable(signal.getNormalizedName(), sv.getNormalizedValue());
      } else {
        // Default to 1.0 for missing signals (neutral in multiplication)
        localExpression.setVariable(signal.getNormalizedName(), 1.0);
      }
    }

    // Evaluate formula
    double finalScore;
    try {
      finalScore = localExpression.evaluate();

      // Guard against NaN or Infinity from formula evaluation (e.g., division by zero)
      if (Double.isNaN(finalScore) || Double.isInfinite(finalScore)) {
        log.warn(
            "Formula evaluation returned {} for document {}, using neutral value",
            finalScore,
            hit.getId());
        finalScore = 1.0;
      }
    } catch (Exception e) {
      log.warn("Error evaluating formula for document {}: {}", hit.getId(), e.getMessage());
      // Fall back to neutral value (1.0) on error to avoid mixing BM25 scale with rescore scale.
      // Using esScore would put this document at BM25 scale (0-500) while others are at
      // rescore scale (typically 1-4), causing unpredictable ordering.
      finalScore = 1.0;
    }

    return RescoreResult.builder()
        .documentId(hit.getId())
        .bm25Score(esScore)
        .finalScore(finalScore)
        .formula(formula)
        .signals(signalValues)
        .build();
  }

  /**
   * Get the formula string.
   *
   * @return the exp4j formula
   */
  public String getFormula() {
    return formula;
  }

  /**
   * Get the signal definitions.
   *
   * @return list of signal definitions
   */
  public List<SignalDefinition> getSignals() {
    return signals;
  }
}
