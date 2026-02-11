package com.linkedin.metadata.search.rescore.functions;

import net.objecthunter.exp4j.function.Function;

/**
 * Linear decay function for exp4j.
 *
 * <p>Used for recency scoring: decay(ageInDays, scale, min, max)
 *
 * <p>Formula: max - (ageDays / scale) * (max - min), clamped to [min, max]
 *
 * <p>This produces linear decay: - Returns max when age is 0 - Returns min when age >= scale -
 * Linearly interpolates between
 */
public class LinearDecayFunction extends Function {

  public LinearDecayFunction() {
    super("decay", 4); // decay(ageInDays, scale, min, max)
  }

  @Override
  public double apply(double... args) {
    double ageInDays = args[0];
    double scale = args[1];
    double min = args[2];
    double max = args[3];

    // Guard against zero or negative scale (would cause division by zero)
    if (scale <= 0) {
      return min;
    }

    // Linear decay from max to min over scale days
    double decayed = max - (ageInDays / scale) * (max - min);

    // Clamp to [min, max]
    return Math.max(min, Math.min(max, decayed));
  }
}
