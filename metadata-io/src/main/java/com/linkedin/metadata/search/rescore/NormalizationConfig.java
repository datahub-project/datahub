package com.linkedin.metadata.search.rescore;

import lombok.Builder;
import lombok.Value;

/**
 * Configuration for normalizing a signal value to a bounded range.
 *
 * <p>For SIGMOID normalization, the parameters work as follows:
 *
 * <ul>
 *   <li>inputMin: Values at or below this map to outputMin
 *   <li>inputMax: Values at or above this map to outputMax
 *   <li>steepness: Controls the slope of the S-curve (default 6.0 uses 90% of sigmoid range)
 * </ul>
 */
@Value
@Builder
public class NormalizationConfig {
  /** Type of normalization to apply */
  @Builder.Default NormalizationType type = NormalizationType.NONE;

  /** Minimum input value for sigmoid (values below map to outputMin) */
  @Builder.Default double inputMin = 0.0;

  /** Maximum input value for sigmoid (values above map to outputMax) */
  @Builder.Default double inputMax = 1000.0;

  /**
   * Steepness of sigmoid curve. Higher = sharper transition.
   *
   * <ul>
   *   <li>1.0 = very gentle, almost linear
   *   <li>6.0 = balanced S-curve (default, uses ~90% of range)
   *   <li>10.0 = sharp, nearly step function
   * </ul>
   */
  @Builder.Default double steepness = 6.0;

  /** Scale factor for decay (for LINEAR_DECAY, e.g., days) */
  @Builder.Default double scale = 180.0;

  /** Minimum output value after normalization */
  @Builder.Default double outputMin = 1.0;

  /** Maximum output value after normalization */
  @Builder.Default double outputMax = 2.0;

  /** Value to use when boolean signal is true */
  @Builder.Default double trueValue = 1.0;

  /** Value to use when boolean signal is false */
  @Builder.Default double falseValue = 1.0;

  /**
   * Backward compatibility: get cap as inputMax.
   *
   * @deprecated Use inputMax instead
   */
  @Deprecated
  public double getCap() {
    return inputMax;
  }

  /** Create a sigmoid normalization config with default steepness */
  public static NormalizationConfig sigmoid(double inputMax, double outputMin, double outputMax) {
    return sigmoid(0.0, inputMax, 6.0, outputMin, outputMax);
  }

  /** Create a sigmoid normalization config with full parameters */
  public static NormalizationConfig sigmoid(
      double inputMin, double inputMax, double steepness, double outputMin, double outputMax) {
    return NormalizationConfig.builder()
        .type(NormalizationType.SIGMOID)
        .inputMin(inputMin)
        .inputMax(inputMax)
        .steepness(steepness)
        .outputMin(outputMin)
        .outputMax(outputMax)
        .build();
  }

  /** Create a log-sigmoid normalization config (for large value ranges) */
  public static NormalizationConfig logSigmoid(
      double inputMin, double inputMax, double steepness, double outputMin, double outputMax) {
    return NormalizationConfig.builder()
        .type(NormalizationType.LOG_SIGMOID)
        .inputMin(inputMin)
        .inputMax(inputMax)
        .steepness(steepness)
        .outputMin(outputMin)
        .outputMax(outputMax)
        .build();
  }

  /** Create a linear decay normalization config */
  public static NormalizationConfig linearDecay(double scale, double outputMin, double outputMax) {
    return NormalizationConfig.builder()
        .type(NormalizationType.LINEAR_DECAY)
        .scale(scale)
        .outputMin(outputMin)
        .outputMax(outputMax)
        .build();
  }

  /** Create a boolean normalization config */
  public static NormalizationConfig bool(double trueValue, double falseValue) {
    return NormalizationConfig.builder()
        .type(NormalizationType.BOOLEAN)
        .trueValue(trueValue)
        .falseValue(falseValue)
        .build();
  }

  /** Create a pass-through (no normalization) config */
  public static NormalizationConfig none() {
    return NormalizationConfig.builder().type(NormalizationType.NONE).build();
  }
}
