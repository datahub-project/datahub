package com.linkedin.util;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;


public class PairCoercer implements DirectCoercer<Pair> {
  static {
    Custom.registerCoercer(new PairCoercer(), Pair.class);
  }

  @Override
  public Object coerceInput(Pair object) throws ClassCastException {
    return object.toString();
  }

  @Override
  public Pair coerceOutput(Object object) throws TemplateOutputCastException {
    String pairStr = (String) object;
    String[] split = pairStr.split(",");
    return Pair.of(split[0].substring(1), split[1].substring(0, split[1].length() - 1));
  }
}
