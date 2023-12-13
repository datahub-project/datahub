package com.linkedin.metadata;

import com.datahub.test.TestEntitySnapshot;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;

public class TestEntitySpecBuilder {

  private TestEntitySpecBuilder() {}

  public static EntitySpec getSpec() {
    return new EntitySpecBuilder().buildEntitySpec(new TestEntitySnapshot().schema());
  }
}
