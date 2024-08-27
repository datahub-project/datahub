package com.datahub.test.testing.urn;

import com.linkedin.data.template.Custom;

public class BarUrnCoercer extends BaseUrnCoercer<BarUrn> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new BarUrnCoercer(), BarUrn.class);
}
