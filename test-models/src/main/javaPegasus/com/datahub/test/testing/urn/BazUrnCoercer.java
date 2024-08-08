package com.datahub.test.testing.urn;

import com.linkedin.data.template.Custom;

public class BazUrnCoercer extends BaseUrnCoercer<BazUrn> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new BazUrnCoercer(), BazUrn.class);
}
