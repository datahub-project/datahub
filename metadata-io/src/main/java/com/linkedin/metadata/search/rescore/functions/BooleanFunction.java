package com.linkedin.metadata.search.rescore.functions;

import net.objecthunter.exp4j.function.Function;

/**
 * Boolean signal function for exp4j.
 *
 * <p>Maps a boolean-like value to configured true/false values: bool(value, trueValue, falseValue)
 *
 * <p>Returns trueValue if value > 0, otherwise falseValue.
 */
public class BooleanFunction extends Function {

  public BooleanFunction() {
    super("bool", 3); // bool(value, trueValue, falseValue)
  }

  @Override
  public double apply(double... args) {
    double value = args[0];
    double trueValue = args[1];
    double falseValue = args[2];

    return value > 0 ? trueValue : falseValue;
  }
}
