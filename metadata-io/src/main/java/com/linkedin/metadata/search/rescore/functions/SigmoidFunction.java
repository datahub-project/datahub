package com.linkedin.metadata.search.rescore.functions;

import net.objecthunter.exp4j.function.Function;

/**
 * Sigmoid normalization function for exp4j.
 *
 * <p>Maps a value to a bounded range using sigmoid curve: sigmoid(value, cap, min, max)
 *
 * <p>Formula: min + (max - min) * (1 / (1 + exp(-scaled))) where scaled = (capped / cap * 6) - 3
 *
 * <p>This produces an S-curve that: - Approaches min when value is 0 - Approaches max when value >=
 * cap - Has steepest slope around cap/2
 */
public class SigmoidFunction extends Function {

  public SigmoidFunction() {
    super("sigmoid", 4); // sigmoid(value, cap, min, max)
  }

  @Override
  public double apply(double... args) {
    double value = args[0];
    double cap = args[1];
    double min = args[2];
    double max = args[3];

    // Guard against zero or negative cap (would cause division by zero)
    if (cap <= 0) {
      return min;
    }

    // Cap the value
    double capped = Math.max(0, Math.min(value, cap));

    // Scale to [-3, 3] range for sigmoid
    double scaled = (capped / cap * 6.0) - 3.0;

    // Apply sigmoid
    double sig = 1.0 / (1.0 + Math.exp(-scaled));

    // Map to output range
    return min + (max - min) * sig;
  }
}
