package com.datahub.test.testing.urn;

import com.linkedin.data.template.Custom;

public class PizzaUrnCoercer extends BaseUrnCoercer<PizzaUrn> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new PizzaUrnCoercer(), PizzaUrn.class);
}
