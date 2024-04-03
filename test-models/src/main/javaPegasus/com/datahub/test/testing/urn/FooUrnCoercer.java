package com.datahub.test.testing.urn;

import com.linkedin.data.template.Custom;

public class FooUrnCoercer extends BaseUrnCoercer<FooUrn> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new FooUrnCoercer(), FooUrn.class);
}
