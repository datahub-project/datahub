package com.linkedin.metadata.search.hybrid;

/** Normalizes lexical relevance into the centered score consumed by {@link HybridScoreCombiner}. */
public class HybridLexicalScoreNormalizer {

  public static final double DEFAULT_INPUT_MIN = 0d;
  public static final double DEFAULT_INPUT_MAX = 500d;
  public static final double DEFAULT_STEEPNESS = 6d;

  private final double inputMin;
  private final double inputMax;
  private final double steepness;

  public HybridLexicalScoreNormalizer() {
    this(DEFAULT_INPUT_MIN, DEFAULT_INPUT_MAX, DEFAULT_STEEPNESS);
  }

  public HybridLexicalScoreNormalizer(
      final double inputMin, final double inputMax, final double steepness) {
    if (inputMax <= inputMin) {
      throw new IllegalArgumentException("inputMax must be greater than inputMin");
    }
    if (steepness <= 0d) {
      throw new IllegalArgumentException("steepness must be positive");
    }
    this.inputMin = inputMin;
    this.inputMax = inputMax;
    this.steepness = steepness;
  }

  public double normalize(final double lexicalScore) {
    final double clampedScore = Math.max(inputMin, Math.min(lexicalScore, inputMax));
    final double unitScore = (clampedScore - inputMin) / (inputMax - inputMin);
    return (unitScore - 0.5d) * steepness;
  }

  public double getInputMin() {
    return inputMin;
  }

  public double getInputMax() {
    return inputMax;
  }

  public double getSteepness() {
    return steepness;
  }
}
